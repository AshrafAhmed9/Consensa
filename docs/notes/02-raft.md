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
the hosting state machine. A hand-rolled, length-prefixed TCP adapter now carries Raft
messages without contaminating `Node` with I/O. `Host` combines that adapter, the durable
Ready ordering, and one apply callback; a two-process integration test exercises it over
localhost. It is still a single-group runtime and is not yet the public vector node's
deployment path. The checker is only for tiny register histories, not a proof of all
database behavior.
