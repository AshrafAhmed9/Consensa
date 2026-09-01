# ADR 007: pre-vote does not defend against a persistent asymmetric partition

## Decision

Ship the current pre-vote implementation (`internal/raft/election.go`) as-is. Document,
rather than fix, a real gap: it stops a node from disrupting a healthy cluster after
*rejoining* from an isolation, but not a node that is *continuously* cut off from the
leader alone while still fully connected to the rest of the group. Closing that gap
means adding etcd-style leader-stickiness (`CheckQuorum`), which is new protocol
surface, not a bug fix — it gets its own phase if the plan calls for it.

## Context

While trying to get `harness/torture` to catch a deliberately weakened pre-vote check
(`docs/notes/06-torture.md`), every fault the harness could generate was a *symmetric*
isolation: cut a target off from everyone, or from no one. Real elections and pre-vote
both grant a vote based only on log freshness (`handleVote`/`handlePreVote` in
`election.go`) — neither checks whether the responder currently has a healthy,
reachable leader. That is exactly Raft's original design (§5.2); pre-vote (Ongaro
thesis §9.6) only adds "don't let a *reconnecting* node's inflated term disrupt anyone,"
by not bumping the candidate's real term until it already knows it can win.

Testing an *asymmetric* partition — one node (call it D) cut off from the current
leader only, still fully connected to every other follower — exposes a scenario neither
mechanism protects against, and neither the paper's base algorithm nor this pre-vote
implementation was ever designed to. `TestAsymmetricPartitionDisruptsHealthyLeader`
(`internal/raft/cluster_test.go`) proves this two ways in one test: with pre-vote
correctly implemented, D still wins real elections against the followers who can
reach it and repeatedly displaces the actual leader, purely because those followers
have no way to know a perfectly healthy leader still exists — they only see a candidate
with an up-to-date log asking for a vote. Weakening pre-vote's freshness check further
does not change this outcome, which is the actual reason
`docs/notes/06-torture.md`'s deliberately-injected-bug experiments for pre-vote never
found a schedule that distinguished weakened from correct: full bidirectional
isolation (what the harness generates) can never produce this scenario, and the
scenario that does produce it doesn't distinguish the two implementations anyway.

## Consequences

- The harness's DoD ("catch a deliberately weakened pre-vote check") is not achievable
  by generating harder isolation schedules of any kind — the class of bug pre-vote
  guards against (a *reconnecting* node) is different from the class this session was
  probing (a *persistently* asymmetric partition), and no fault schedule collapses that
  distinction. Retire that specific DoD item rather than keep spending seeds on it; a
  scripted, deterministic test (as added by this ADR) is the correct tool for a
  narrow protocol property like this, the same way `TestFigure8CommitRule` is a scripted
  unit test rather than something the torture harness is expected to stumble into.
- The disruptive-server-under-persistent-partition gap is real and would matter in
  production: a `docker network` rule cutting one node off from its leader only (not
  from its peers) can cause a live cluster to keep re-electing leaders, degrading
  availability, until the partition heals. `docs/correctness.md` states this plainly
  as a known limitation rather than a claim.
- If this is worth closing later, the fix is well understood and bounded: gate
  `handleVote`/`handlePreVote` on the responder's own `electionElapsed` — reject any
  vote (real or pre-) while `electionElapsed < electionTick` and the responder has a
  known leader, mirroring etcd's `CheckQuorum`. Not implemented here because it is a
  protocol-level addition deserving its own review and test suite, not a two-line
  patch bundled into a torture-harness session.
