# Phase 7: static ranges and routing

## Why does this exist?

One Raft group caps a cluster at one leader’s capacity. Ranges give different key spans
independent replication and leadership while preserving ordered lookup.

## How does it work?

Descriptors map half-open spans to replica sets. Metadata validates non-overlap, and a
router caches lookups until a stale-descriptor response makes it refresh.

## What alternatives existed?

A global hash ring distributes load but loses contiguous ordered spans and makes range
scans awkward. Static ranges establish mechanics before dynamic policy exists.

## What tradeoff was made?

The router sends a request to the elected leader of its resolved range. Each range owns a
small, namespaced state machine whose mutations are applied only after Raft commits them;
this makes replica convergence testable without pretending a direct in-memory map is
replicated. The scheduler ticks independent static range groups together.

`MultiRaft`/`rangeState` is still an in-memory assembly, not a live node implementation --
that remains true and is unchanged. What changed: `DurableRange` (`durable_range.go`) is a
second state machine driven by the identical wire format, applying the same commands as
real MVCC writes into a real `storage.Engine` behind a real `raft.Host`/TCP connection.
`TestDurableRangeSurvivesRestart` proves a killed-and-restarted range replica recovers its
key/value data from disk before contacting any peer, and `TestDurableRangeDeleteIsDurable`
proves a delete survives the same restart (not just a put -- a range whose apply only ever
replayed puts on recovery would resurrect deleted keys, and this test would have caught
that). Unlike `internal/ann.DurableNode`'s HNSW graph, a byte KV range has no in-memory
structure to rebuild, so recovery is just `storage.Engine`'s own WAL/SSTable recovery --
`DurableRange` adds no recovery logic of its own, which is the point of building it that
way rather than copying `DurableNode`'s replay pattern verbatim.

Still genuinely missing, unchanged in kind from before: **one range still means one Raft
transport** (one TCP listener, one storage directory) -- `DurableRange` proves the
per-range durability story, not the multi-range multiplexing the plan actually calls for
(many ranges sharing one transport with batched cross-range heartbeats). `DurableRange` is
also not yet wired into `Router`/`RoutedKV` the way `DurableNode` is wired into
`cmd/consensa/main.go` -- there is no durable, routed, multi-range binary yet, only a
durable single-range replica type with its own standalone test. Replica movement and
dynamic range policy also remain later work.

## What can fail?

Clients can use a stale descriptor transiently; the explicit mismatch and refresh path is
the recovery contract. Overlapping descriptors are rejected on creation. `DurableRange`
shares one `storage.Engine` between range data and `Persister`'s own bookkeeping (hard
state, log entries, snapshots), which live under a reserved `"raft/"` key prefix in the
same engine -- `Put`/`Delete` reject a user key with that prefix outright rather than
silently letting it corrupt Raft's own durable state.
