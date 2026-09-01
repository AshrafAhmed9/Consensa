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

`TestRouterDirectsRealRangesAndIsolatesFailure` closes the routing half of the gap: two
independent ranges, each a real 3-node `DurableRange` group over real TCP, with `Router`
resolving keys to the correct range by key boundary. It proves three things at once --
`Router` really does send different keys to different ranges' real leaders (not just an
in-memory `Cluster` standing in for one); a key never lands on the wrong range's replicas
(cross-contamination would mean the boundary check was decorative); and killing range A's
leader has *zero* effect on range B's leadership or term, proving range isolation under a
real failure rather than assuming it from the descriptors being logically separate.

This deliberately does **not** add a server-side "find the leader of this range and
forward" abstraction to `DurableRange` or a wrapper type. `internal/ann.DurableNode`
already established that retrying across replicas is the client's job (see
`docs/notes/05-api.md`); giving ranges a different contract here would be an
inconsistency introduced for convenience, not a real simplification. The test's own
`routedPut` helper plays the client's role, the same way `upsertUntilAccepted` does in
`cmd/consensa/main_e2e_test.go`.

Still genuinely missing, unchanged in kind from before: **one range still means one Raft
transport** (one TCP listener, one storage directory) -- proven routing and proven
isolation are not the same claim as the multi-range multiplexing the plan actually calls
for (many ranges sharing one transport with batched cross-range heartbeats). Nothing
durable is wired into a real binary yet either -- `cmd/consensa` still only runs a single
`DurableNode`, not a routed, multi-range deployment. Replica movement and dynamic range
policy also remain later work.

## What can fail?

Clients can use a stale descriptor transiently; the explicit mismatch and refresh path is
the recovery contract. Overlapping descriptors are rejected on creation. `DurableRange`
shares one `storage.Engine` between range data and `Persister`'s own bookkeeping (hard
state, log entries, snapshots), which live under a reserved `"raft/"` key prefix in the
same engine -- `Put`/`Delete` reject a user key with that prefix outright rather than
silently letting it corrupt Raft's own durable state.
