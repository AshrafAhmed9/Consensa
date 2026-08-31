# Phase 0: foundations

## Why does this exist?

Consensus bugs are scheduling bugs. A test that passes once on the wall clock does not
give a useful explanation when it fails tomorrow. The simulator makes a seed and fault
configuration a complete, replayable execution.

## How does it work?

`Scheduler` is single-threaded. Endpoints enqueue copied byte slices; each scheduler tick
advances all fake clocks, orders due messages by deterministic sequence number, and
delivers them. Faults are selected by a seeded PCG generator. A partition permits traffic
only inside its declared groups.

## What alternatives existed?

Real TCP integration tests, goroutines, and sleeps are simpler to start with. They make
interleavings depend on the host scheduler and cannot reliably reproduce a failing
network schedule.

## What tradeoff was made?

Every future network and clock boundary needs an interface, and tests must explicitly
tick time. That ceremony buys deterministic tests and a small model that is easy to
reason about.

## What can fail?

The simulator can model only faults it is asked to model; it cannot prove operating-system
or disk behavior. It also assumes callers do not race the scheduler. The Phase 2 Raft
tests will make that constraint valuable.
