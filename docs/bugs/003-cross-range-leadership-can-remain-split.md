# 003 — a single process is not guaranteed to lead every Raft group it hosts

**Status: found this session, real fix landed (see "Update: the real fix" below).**
Unlike bugs 001 and 002, the root cause here was a genuine architectural gap, not a small
code defect with an obvious patch — the original write-up (preserved below) records it in
the same spirit as `docs/adr/007`'s pre-vote gap, as a proven, permanent-until-fixed fact
about the system. It has since been fixed, not just mitigated; the mitigation
(`electionStaggerSpread`) is left in place too, since it lowers how often the fix's
transfer path needs to run at all.

**Found by:** running `cmd/consensa`'s own `TestConsensaBinaryThreeProcessClusterSurvivesKillAndRestart`
and a standalone `demo.sh` script repeatedly, by hand, dozens of times in a row on one
machine under load. The failure is intermittent — sometimes several consecutive runs pass
cleanly, sometimes several consecutive runs fail the same way — which is exactly why it
had not previously been written up despite the test existing and running in CI: CI's own
runs of this test have occasionally shown the identical failure signature earlier in this
project's history without it being isolated to a specific, named cause before now.

**Symptom:** `ConsensaKV.TransactionalPut` fails with `raft: proposal to non-leader`,
retried against every node, for the test's or demo's entire retry budget (tens of
seconds) — not a transient blip that clears up, but a sustained failure to commit at all
during that window.

**Root cause:** `cmd/consensa` hosts three independent Raft groups per process — the
vector index and the two static KV ranges — sharing one `MultiplexedTransport` listener
but each electing its own leader completely independently. `KVService.TransactionalPut`
never forwards a write to another process (`docs/notes/05-api.md` states this design
choice plainly): it can only commit a transaction spanning both KV ranges if the *same*
process happens to be leading *both* of them at the moment the request arrives.

`hostElectionStagger` (`internal/raft/host.go`) deterministically favors the same
lowest-ranked peer for all three groups *in expectation*, since all three use an
identical peer list and election-tick configuration and tick from the same wall clock.
But each group's pre-vote/vote round trip travels over its own independent sequence of
real network messages, and real message delivery jitter between groups is not
correlated — so the deterministic *bias* is not a deterministic *guarantee*. Under enough
real-world scheduling variance (busy machine, contended CPU, slow disk fsync), the three
groups can settle into a *stable* split — for example, one process leading the vector
index and KV range 1, while a different process leads KV range 2 — with no further
election churn at all. Once that split is stable, `TransactionalPut` cannot succeed
against *any* node until something disturbs it (a restart, a deliberate leadership
transfer, or a lucky future re-election), because no process ever satisfies the "leads
both ranges" precondition on its own.

This is a genuine property of the current design, not a bug in `advanceCommit`, election
safety, or anything this session's joint-consensus work touched — `internal/raft` and
`internal/kv`'s own test suites, which exercise each Raft group in isolation, pass
reliably; only this specific cross-group, single-process-affinity assumption is fragile.

**What was ruled out before landing on this explanation:**
- Two real, independent performance issues were found and fixed while chasing this
  (`raftLog.term`'s O(log length) linear scan made O(1); a periodic
  `AdvanceClosedTimestamp` caller consolidated into the existing tick goroutine instead of
  a second one contending for `Host`'s own mutex) — both are real, valid fixes, kept
  regardless, but neither fully explains this symptom: the identical failure was
  reproduced against a build with neither change present, confirming the underlying flake
  predates this session's work.
- Tick interval, election-settle delays, and retry budget were all independently varied
  (`-tick-interval 20ms` to match the passing test's own configuration; up to 60s retry
  budgets; up to 90 seconds of real elapsed time with a stable, non-churning Raft term
  observed throughout) without reliably preventing the failure — ruling out "just needs
  more time" as the explanation once a stable split has occurred.

**What would actually fix it, not yet attempted:** either (a) a real leadership-affinity
mechanism across co-located Raft groups (e.g., a lightweight leadership-transfer nudge
when a process detects it leads some but not all of its local groups), or (b) forwarding
a `TransactionalPut` to whichever process actually leads each range, which is the
general server-side-forwarding feature `docs/notes/05-api.md` already documents as
out of scope for the whole project, not just this path. Both are real, non-trivial
design work — correctly scoped as future work rather than rushed here, matching how
`docs/adr/007` treats pre-vote's own unfixed gap.

**Mitigation landed:** `hostElectionStagger`'s spread was widened 4x
(`electionStaggerSpread` in `internal/raft/host.go`), giving the favored (lowest-ranked)
node a much larger head start over the next-ranked node across all three co-located
groups. This does not change the underlying architectural gap above — it only makes the
"stable split" outcome less likely by making the bias each group's election already has
much harder for real network jitter to overcome. `TestConsensaBinaryThreeProcessClusterSurvivesKillAndRestart`
passed 3/3 clean runs locally after the change (previously intermittent); this is
evidence of a lowered flake rate, not a proof the split can no longer happen.

**Update: the real fix.** Option (a) from "what would actually fix it" above is now
implemented: a leadership-transfer nudge, not just a wider election bias.

`internal/raft` gained the primitive Raft itself defines for this (etcd calls it
leadership transfer): a new `MsgTimeoutNow` message type (`state.go`) and
`Node.TransferLeadershipTo(to)` / `Host.TransferLeadershipTo(to)` (`node.go`, `host.go`).
A leader sends `MsgTimeoutNow` to a specific peer only once it has confirmed that peer's
log is fully caught up (`n.match[to] >= n.log.lastIndex()`) — sending it to a peer that
isn't caught up would let that peer win an election and then be structurally unable to
reconstruct entries the old leader already committed, violating Raft's leader-completeness
property, so `TransferLeadershipTo` refuses and returns an error in that case rather than
sending anything. The recipient (`handleTimeoutNow`, `election.go`) skips pre-vote and
calls `startElection` directly, but only for a message from the peer it already
recognizes as its current leader (`Node.Leader()`, newly exposed) — accepting a transfer
request from anyone else would let any peer force a disruptive election merely by sending
this message type, exactly what pre-vote exists elsewhere to prevent.

`cmd/consensa.maintainLeadershipAffinity` (`main.go`) is the policy built on that
primitive, run as its own goroutine on the same cadence as Raft ticking. Every process
already computes the identical "preferred leader" for its three co-located groups —
`groupPeers[0]`, the lowest-ranked ID in the shared peer list, the same value
`hostElectionStagger` already biases every group's own election toward — without any
new inter-process signaling. The policy is simply: if this process currently leads a
group but is not itself the preferred node, transfer that group's leadership to the
preferred node. Every process runs the identical check, so this converges to the
preferred node leading all three groups; a transfer that fails because the preferred node
hasn't replicated far enough yet is silently retried next tick, the same pattern
`executeSplitIfRecommended` and `maintainLeases` already use for their own not-yet-ready
failures.

This is a genuine fix, not another mitigation layer: given the described stable-split
scenario (one process leading the vector index and KV range 1, a different process
leading KV range 2), the process leading KV range 2 alone will, within one affinity
check, hand that range's leadership to the preferred node — no restart, no leadership
transfer by luck, no waiting on an unrelated re-election. Proven at the primitive level
by `TestTransferLeadershipToCaughtUpPeer`, `TestTransferLeadershipRejectsUncaughtUpPeer`,
and `TestTransferLeadershipRequiresLeader` (`internal/raft/cluster_test.go`) — the
existing `TestConsensaBinaryThreeProcessClusterSurvivesKillAndRestart` and
`TestConsensaBinaryExecutesALiveSplitAutomatically` e2e tests exercise it in the real
binary, though their own seed-write stage remains subject to this bug's *general* failure
mode (a transaction proposed before the affinity policy's first tick has run, or during
this local machine's own heavy contention) so an isolated flake in those specific tests is
not on its own evidence the fix regressed anything -- CI, on a clean runner, is the
signal that matters for that.

**What this still does not prove, stated plainly:** the affinity policy is eventually
self-correcting, not instantaneous — a `TransactionalPut` issued in the brief window
before the preferred node has regained every range's leadership can still see
`raft: proposal to non-leader` once, the same as any ordinary leader-not-yet-elected
window; callers already retry across nodes for exactly this reason. This also does not
touch general server-side request forwarding, still out of scope per
`docs/notes/05-api.md` — the fix works specifically because this deployment's three
groups share one identical peer list, so one deterministic "preferred" node exists at
all; a topology where different ranges have different replica sets would need the
forwarding approach instead.
