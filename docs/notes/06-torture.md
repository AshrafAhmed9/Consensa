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
