# Phase 6: torture harness

## Why does this exist?

Distributed failures need a reproducible schedule, not a vague report that a test failed
under load. The harness makes the seed, workload, and faults first-class artifacts.

## How does it work?

Seeded fault schedules, JSON histories, a register linearizability checker, and vector
invariants are exposed through a small CLI. A failing run leaves replayable input behind.

## What alternatives existed?

Black-box chaos tools exercise a running cluster but cannot enumerate every simulated
delivery choice or necessarily replay a failure.

## What tradeoff was made?

The initial harness is intentionally narrow and deterministic. It grows by connecting
each real service path rather than pretending reference workloads prove production behavior.

## What can fail?

A passing checker establishes only the property it encodes. The correctness document
states those boundaries explicitly and must change with test coverage.

**Until this session, `register.run()` checked a fixed, hand-written two-operation
history that never touched Go and never depended on the seed or nemesis schedule at
all** -- `torture run --workload register --seeds 5000` would have repeated the identical
hardcoded check 5000 times and always passed, regardless of whether Consensa's actual
Raft implementation had a bug. `cmd/torture` (new) fixes this: it drives a real
`internal/raft.Cluster` under the seed's fault schedule (using two new exported methods,
`Cluster.TickFiltered`/`ProposeFiltered` -- external callers could not previously inject
faults at all, since `Cluster.Tick`/`Propose` always deliver every message and `c.nodes`
is unexported) and prints a real client-observable operation history, which
`register.run()` now feeds to the existing `is_linearizable` checker.

**The harness demonstrably observes real effects of real faults.** Isolating a node whose
term-limited self-belief hasn't yet expired (a "zombie leader") produces the expected
result: a proposal to it appends locally but the driver correctly does not record it as a
successful write (it checks the leader's own `Applied()` tail, not just a nil error from
`Propose` -- an earlier version of this tool trusted the nil error alone and would have
recorded phantom successful writes, which was a bug in the test client's own modeling, not
a discovery about Consensa). Reads during the same isolation window correctly return the
last value that actually committed, not the pending one -- proving the fault injection is
real, not decorative.

**What this session tried and could not close: "the harness itself is tested," per this
phase's own DoD.** Matching the same discipline used to validate the TLA+ specs and the
`raft.Host` fix earlier in this project, the plan requires proving the harness catches a
deliberately injected bug. Two were tried against 500+ seeds each (weakening the Figure-8
commit rule in `advanceCommit`; disabling the pre-vote log/term-freshness check entirely)
and **neither was caught**, even though `TestFigure8CommitRule` (a white-box unit test
that scripts the exact log/match state directly) catches the first one immediately.

**Update: sustained fault windows were implemented, and they surfaced a real bug --
just not the one they were built to find.** `nemesis.schedule()` now generates
multi-round isolation windows (4-10 consecutive rounds, `MIN_WINDOW`/`MAX_WINDOW` in
`nemesis.py`) instead of independent single-round events. Running the register workload
under this against the *correct* implementation immediately produced non-linearizable
histories in roughly 15% of seeds -- not a flake, a real bug: `Cluster.Leader()`
(`internal/raft/cluster.go`) picked among nodes with `role == Leader` using Go's
undefined map iteration order. During a sustained isolation, the isolated ex-leader keeps
believing it is leader (a genuine "zombie leader" -- this is not itself a bug, it is
exactly what Raft's safety properties are designed to tolerate) while the reachable
majority elects a real replacement at a higher term. `Leader()` could return either one
depending on map iteration, so the driver's picture of "the leader" could flip backwards
between rounds with no fault or bug involved. **Fixed** by breaking the tie on highest
term, which is always the node a real client would actually observe as current. This is
now a permanent fix in `Cluster.Leader()`, benefiting every caller
(`ann.ReplicatedIndex`, `kv.multiraft`, this harness) -- not a torture-harness-only patch.

With that false-positive source removed, the harness runs clean across 200+ seeds against
the correct implementation, confirming sustained windows alone did not just trade one kind
of flakiness for another.

**But re-running the same deliberate-bug experiment (Figure-8, pre-vote) against the fixed
`Leader()` caught neither bug, at any of 400 seeds, at 3 or 5 nodes, at up to 60 steps.**
The earlier "caught at seed 7" result for *both* injected bugs, reported before this
session found the `Leader()` bug, was that same false positive -- coincidence, not
detection, which is why both experiments hit at the identical seed. Root-caused this
time by tracing `cmd/torture`'s actual leader/term sequence for seed 7: node 0, the
initial leader, gets isolated for the whole first fault window, and the majority
correctly elects a replacement within 3 rounds and keeps committing writes throughout --
completely correct behavior, and exactly why neither weakened check ever fires.
`Node.Tick()` only calls `startPreVote` when `role != Leader`; an isolated node that
already believes it is leader never attempts a re-election at all, so weakening
`handlePreVote`'s freshness check is inert unless the *isolated node is a follower*, not
whichever node the schedule happens to isolate. Even isolating a follower is not
sufficient by itself: the disruption pre-vote exists to prevent only shows up when that
follower has been repeatedly incrementing its own term while cut off and then
*reconnects* while a healthy leader is still up, at a moment when its inflated term can
reach that leader's followers. `nemesis.schedule()` picks its target uniformly at random
each window with no notion of current role, so it has no mechanism to reliably produce
that sequence -- sustained windows were necessary to get real elections happening at all
(and they do now, correctly, which the `Leader()` fix confirms), but they are not
sufficient to exercise this specific class of election-safety bug.

**The precise next step, now that the vaguer "single-round" theory is retired:** the
schedule generator needs to target the current *non-leader* role specifically (which
requires either querying live cluster state when generating faults -- a real change to
the "pure function of the seed" design -- or a purpose-built adversarial schedule that
scripts a known-follower isolation-then-reconnect sequence, closer to how
`TestFigure8CommitRule`'s unit test scripts exact log/match state directly). That is a
non-trivial harness design decision, not a parameter tweak, which is why it remains open
rather than attempted in this pass.
