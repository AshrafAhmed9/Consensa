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
- A change cannot introduce a previously unknown process: each target must already be in
  the static `Config.Peers` transport universe. Bootstrap, address distribution, and
  range-routing changes remain separate work.
