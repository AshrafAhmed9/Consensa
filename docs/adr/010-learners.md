# ADR 010: learners first, then live joint consensus

## Context

Learners were added before dynamic membership so a replica can catch up without entering
the voting quorum. The next step is joint consensus: a configuration entry changes the
quorum used by both elections and commits, which is safety-critical because a mistake can
silently permit disjoint leaders.

PLAN.md's Phase 11 section names the reason a naive "just add the new node as a voter"
membership change is unsafe on its own, independent of the joint-consensus dual-majority
problem: a brand-new, empty replica added directly as a full voter immediately makes
quorum() require a majority that includes a replica with no log yet, which *weakens*
fault tolerance the moment it's added, rather than strengthening it. Real Raft
implementations (etcd included) solve this specific piece with **learners**: non-voting
replicas that receive log replication (so they can catch up) but are never counted toward
`quorum()` and never vote, promoted to full voters only once caught up.

## Decision

Learners remain non-voting replicas. Promote, demote, and remove already transport-known
peers through log-replicated joint consensus. A joint entry takes effect when appended;
`Membership.HasQuorum` requires a majority of both Old and New for elections and commits;
after that entry commits, the leader appends the final configuration automatically.

The initial learner change remains additive. Joint consensus extends it without treating a
union-majority as safe: during the transition both voter sets independently form a
majority. Membership is reconstructed from the log after every append so a conflicting,
uncommitted config entry is forgotten. `Snapshot.ConfState` carries the effective
membership across log compaction and recovery.

What changed, concretely (`internal/raft/node.go`, `election.go`):
- `Config.Learners` names a subset of `Peers` that are non-voting.
- `advanceCommit()` and election vote counting use `Membership.HasQuorum`; during joint
  consensus it requires a majority of both configurations.
- `startPreVote()`/`startElection()` only message voters -- a learner's vote could never
  legitimately count, so asking is wasted traffic at best.
- `handlePreVote()`/`handleVote()` reject outright if the receiving node is itself a
  learner -- defense in depth: the normal path never asks a learner to vote, but a
  learner voting would silently violate the "quorum counts only voters" invariant
  everywhere else in this file relies on, so it's rejected even under a message the
  protocol would never generate on its own.
- A learner's own `Tick()` never starts an election, no matter how long it goes without
  hearing from a leader -- it could never win one, so trying is pure waste, and (more
  importantly for the property this ADR cares about) it removes any path by which a
  learner could ever become `Candidate`/`Leader`.

`TestLearnerNeverBecomesLeader`, `TestLearnerCannotGrantAVote`,
`TestCommitNeverAdvancesOnLearnerAcksAlone`, and `TestLearnerReceivesReplicatedLog`
(`internal/raft/learner_test.go`) prove these properties directly, including the specific
failure mode learners exist to prevent: a leader plus only a learner acknowledging an
entry is a literal majority of *all* nodes but must not commit, since it is not a
majority of *voters*.

## Consequences

- `raft.Node` and `raft.Host` expose `ProposeConfChange`, but `DurableRange` and the
  shipped binary do not yet provide an authenticated admin API for it.

**Update: a genuinely new, previously-unknown process can now join a live group.**
`ProposeConfChange`'s own eligibility check (every target ID must already be in
`n.peers`) used to make this a hard requirement -- `Config.Peers` was fixed at
construction, so growing a cluster to a physical node the deployment had never addressed
before was explicitly out of scope (this section used to say so). Two new, local,
per-replica primitives close it: `Node.AddKnownPeer`/`Host.AddKnownPeer` extend a
replica's own peer universe so `ProposeConfChange` will accept the new ID at all, and
`Host.AddPeer` (via the new `raft.PeerRegistrar` transport capability, implemented by
both `*TCPTransport` and `*rangeView`) registers the new process's real network address
so a message to it can actually be delivered. Neither call is replicated by Raft's own
commit protocol -- both are local bookkeeping, deliberately mirroring each other, and
both must be invoked on every existing replica, not just the leader, before the
`ProposeConfChange` that adds the new ID to the voter or learner set.
`TestBrandNewProcessJoinsLiveGroupAsLearnerThenVoter` proves the full sequence against
three real, already-running `*Host` processes and a fourth started only afterward, with
its ID and address unknown to any of the first three until these calls run: it joins as
a learner, catches up over a real TCP connection AddPeer just made possible, and is then
promoted to a full voter counted toward quorum.

**Update: a real gRPC admin surface now drives this.** `ConsensaAdmin.AddReplica`/
`PromoteReplica` (`api/consensa/v1/consensa.proto`, `internal/server/admin_service.go`)
expose exactly the two-step sequence above -- `AddReplica` calls `AddKnownPeer`/
`AddPeerAddress` (must be called on every replica, not just the leader);
`PromoteReplica` calls `ProposeConfChange` (only succeeds against the current leader,
the same "route to whoever's in charge" contract `ConsensaKV.TransactionalPut` already
established). Gated by `internal/auth`'s optional shared-secret bearer-token interceptor
like every other RPC in this codebase (off by default; see `docs/notes/13-auth.md`), and
unlike the data-plane RPCs, this service can require its OWN separate token
(`--admin-auth-token`) -- a credential valid for ordinary `Upsert`/`Search`/
`TransactionalPut` traffic is not automatically valid here, proven end to end against the
real binary by `TestConsensaBinaryScopesAdminTokenIndependently`.
`TestAdminServiceAddsAndPromotesReplicaOverGRPC` proves the full membership sequence
entirely over real gRPC (not direct Go calls): a genuinely new 4th `kv.DurableRange`,
unknown to any of the original three until `AddReplica` runs, joins as a learner, catches
up over real replication, and is promoted to a full voter.

**What this still does not do, stated plainly:** any valid admin token authorizes both
`AddReplica` and `PromoteReplica` equally -- there is no finer-grained, per-method scoping
within this one service. Bootstrap (how a fresh process learns which group to even try
joining, i.e. discovering `AddReplica`'s own address to call it) and range-routing
updates after a membership change remain separate, undone work.

A real bug was found and fixed while proving this, caught by real CI (not local, not by
inspection): `AddKnownPeer` originally only appended to `n.peers`, but a currently-leading
replica's `n.next` map -- which `sendAppend` reads as `next := n.next[to]` before
computing `prev := next - 1` -- is otherwise only initialized once, in `becomeLeader`, for
whichever peers were already known at that election. A peer added afterward had no
`n.next` entry, so `next` read as the zero value and `prev := next - 1` underflowed
`uint64` to its maximum value, producing a nonsense `AppendEntries` the new peer could
only ever reject. `AddKnownPeer` now also seeds `n.next[id]` when called on the current
leader, mirroring exactly what `becomeLeader` already does for every peer at election
time.
