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

The root cause is understood precisely, not just suspected: `harness/torture/nemesis.py`'s
`schedule()` generates independent single-round fault events -- each isolation lasts
exactly one `Cluster.TickFiltered` call. `Cluster`'s nodes need 3-5 consecutive
ticks without hearing from a leader (`ElectionTick: 3 + position` in `cluster.go`) before
they even *attempt* their own pre-vote. A one-round isolation is structurally too brief for
any node's election machinery to activate at all, which means the current fault model can
only ever exercise the "everything reconnects before anything interesting happens" regime
-- it cannot destabilize leadership regardless of how many seeds are run, so election-path
bugs (pre-vote, Figure-8) are invisible to it by construction, not by bad luck. Fixing this
needs `nemesis.schedule()` (or a new schedule generator) to produce *sustained* multi-round
fault windows, not more seeds of the current one-round model -- that is the concrete next
step, not "run the existing harness longer."
