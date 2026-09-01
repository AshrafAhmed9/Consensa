# ADR 010: wire learners into the live quorum path, defer joint consensus itself

## Context

`internal/raft/membership.go`'s `Membership`/`HasQuorum` implement the dual-majority math
full joint consensus needs, and have since an earlier session -- unit-tested, but
deliberately never wired into `node.go`'s live `quorum()`/election/commit-advance path,
because that path is the single highest-blast-radius piece of code in this project: a
mistake here doesn't fail loudly, it silently breaks the safety property the entire
"proven, not asserted" thesis rests on.

PLAN.md's Phase 11 section names the reason a naive "just add the new node as a voter"
membership change is unsafe on its own, independent of the joint-consensus dual-majority
problem: a brand-new, empty replica added directly as a full voter immediately makes
quorum() require a majority that includes a replica with no log yet, which *weakens*
fault tolerance the moment it's added, rather than strengthening it. Real Raft
implementations (etcd included) solve this specific piece with **learners**: non-voting
replicas that receive log replication (so they can catch up) but are never counted toward
`quorum()` and never vote, promoted to full voters only once caught up.

## Decision

Wire learners into `node.go`'s live quorum path now; leave the full joint-consensus
dual-majority transition (`Membership.HasQuorum`) unwired, as before.

This is a real, useful slice of Phase 11 with a fundamentally smaller blast radius than
the full feature, for a reason worth stating precisely: **learners only ever shrink the
set of things counted toward quorum**, they never split it into two majorities that must
both hold simultaneously. `quorum()` becomes `len(voters())/2+1` where `voters()` is
`peers` minus `learners` -- for every existing caller that never sets `Config.Learners`,
`voters()` returns exactly `peers` in the same order, so `quorum()` and every safety
property already proven against it (Figure 8, pre-vote, `TestFigure8UnsafeCommitWouldBeOverwritten`,
the full torture/chaos suite) are **byte-for-byte unaffected**. The change is additive
and opt-in, not a modification to the existing voting path's behavior.

What changed, concretely (`internal/raft/node.go`, `election.go`):
- `Config.Learners` names a subset of `Peers` that are non-voting.
- `quorum()` and `advanceCommit()`'s majority computation use `voters()` (peers minus
  learners), not the full peer set -- an entry acknowledged only by learners must never
  be reported committed, since a subsequent election among the real voters could still
  overwrite it.
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

- **Still not done, stated plainly:** promotion (constructing a new `Config` without a
  caught-up node in `Learners`, live, without a restart) is not implemented -- today
  promoting a learner means restarting that replica's `Host` with a new `Config`, not a
  live reconfiguration command. There is no bootstrap/admin path anywhere in this project
  (`kv.DurableRange`, `cmd/consensa`) that actually adds a learner to a running group;
  this closes the `raft.Node` primitive only.
- **Full joint consensus remains unwired**, exactly as before this session: config
  entries in the log, dual-majority `HasQuorum` during a joint transition, and
  disjoint-majority election safety across it are real, unit-tested
  (`internal/raft/membership.go`) but not connected to `quorum()`/`advanceCommit()`. That
  wiring is a fundamentally different risk profile than this one -- it changes what
  counts as a majority *during* a transition, for both old and new voters simultaneously,
  which is exactly the kind of change that needs its own dedicated scripted safety tests
  (mid-transition leader crash, mid-transition partition isolating the new config) before
  it should be trusted, the same way Figure 8 got one rather than being trusted on
  inspection alone. This ADR deliberately does not attempt it.
