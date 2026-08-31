# ADR 002: Deterministic simulation is the primary test environment

## Decision

Protocols receive time through `Clock` and network traffic through `Transport`. Tests use
the seeded `internal/sim` scheduler; protocol algorithms contain no goroutines, timers,
channels, or I/O.

## Rationale

Distributed failures are schedules, not merely input values. A seeded scheduler chooses
drops, duplication, delay, partitions, and clock skew, allowing the exact schedule that
found a failure to become a regression test. Real-process testing remains useful later,
but cannot replace replayable state-machine tests.

The cost is deliberate plumbing at each boundary. It is paid now because it cannot be
reliably retrofitted after direct calls to clocks and sockets spread through the code.
