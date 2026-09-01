# Phase 2: Raft

## Why does this exist?

One node's durable LSM state is not enough when a node can fail. Raft supplies a single
order for commands so a majority can recover the same state machine.

## How does it work?

`Node` is a pure state machine. `Tick` starts pre-vote/election work; `Step` consumes an
RPC; `Ready` tells the caller exactly what must be persisted, sent, and applied. Leaders
replicate suffixes and advance commit only for entries in their current term, which is the
Figure 8 rule. Pre-vote asks whether an election would win before increasing a term.

## What alternatives existed?

An existing Raft library would be shorter but hides the protocol being demonstrated.
Goroutines and network sockets inside the node would make the normal implementation easy
but make the simulator's schedule incomplete.

## What tradeoff was made?

The first group has fixed voters and uses a direct in-memory log. The public `Ready`
boundary makes Phase 1 persistence integration explicit; it is intentionally not hidden
behind an asynchronous storage abstraction. `Persister` can reconstruct hard state, the
log suffix, and snapshot metadata on restart; committed entries are offered again so a
crash between durable commit and application does not lose an update.

## What can fail?

The caller must persist `HardState` and `Entries` before transmitting `Messages`; failing
that ordering breaks the argument Raft relies on. Snapshot *application* still belongs to
the hosting state machine. A hand-rolled, length-prefixed TCP adapter carries Raft
messages without contaminating `Node` with I/O. `Host` combines that adapter, the durable
Ready ordering, and one apply callback. It is still a single-group runtime, and it is not
yet what `cmd/consensa/main.go` runs (that binary still uses the in-memory `Cluster`
harness, not `Host`).

**This adapter had a real bug that its one existing test could not have caught.** A 2-node
happy-path TCP test passed the whole time — but it never removed a node from the cluster,
so it never exercised `Send` failing against an address nothing was listening on.
`driveLocked` returned immediately on the first `Send` error to any peer, which meant a
single unreachable node — an ordinary, expected condition under Raft's own fault model —
silently wedged the *entire* host: no message reached the live peers either,
`CommittedEntries` never got applied, and `Advance()` never ran, so the same doomed
message was re-emitted on every subsequent tick forever. `internal/raft/cluster_test.go`
could not have caught this either, because `Cluster`'s in-memory delivery is a direct
function call that cannot fail the way a TCP dial can — the failure mode was invisible to
every test in the repo until `internal/raft/host_test.go` added a deliberate leader kill
that requires the surviving 2-of-3 majority to elect a new leader and keep committing. See
`docs/bugs/001-*.md`. The fix: a per-message send failure is now skipped rather than
aborting the cycle; only a `Persist` failure (genuine, unrecoverable data loss) still
aborts it.

**Lesson worth keeping, independent of the specific bug:** a happy-path integration test
over a real transport can pass indefinitely while a fault-path bug sits right next to it,
because the two exercise almost entirely different code. "It has an integration test"
is not the same claim as "it has been tested under the failure it exists to survive" —
say which one is actually true before citing a test as evidence.
