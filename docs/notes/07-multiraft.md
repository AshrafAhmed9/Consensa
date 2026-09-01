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

**Update: "one range means one transport" is now half-closed.** `MultiplexedTransport`
(`internal/raft/multiplex.go`) is one real TCP listener per node, shared by every range
Host on it, instead of each range's Host dialing its own dedicated socket. `Host` was
refactored to accept an injectable `Transport` interface (`transport.go`) rather than a
concrete `*TCPTransport`, so this required no change to `Host`'s own protocol logic --
existing callers that don't set `HostConfig.Transport` are unaffected and still get a
dedicated listener, exactly as before.
`TestMultiplexedTransportRoutesRangesIndependently` proves two ranges sharing one real
listener per node (3 nodes, confirmed via `Addr()` returning exactly 3 distinct
addresses, not 6) each elect independently and never cross-contaminate committed
data -- checked by recording every entry each replica actually applies, per range, and
asserting range A's replicas never see range B's proposed value or vice versa, not just
that both ranges' terms progressed. A real, repeatable flake was found and fixed while
building this test: at a 5ms tick interval across two ranges' worth of hosts, dial-per-
message TCP churn on loopback caused elections/replication to fail outright about 40% of
the time; slowing to the 10ms interval this codebase's other TCP-backed tests already use
made it reliable across 16 consecutive runs.

**What this does not close, stated plainly:** batched heartbeat coalescing across ranges
sharing a destination node -- the other half of the plan's "1,000 ranges must not mean
1,000x heartbeat traffic" claim -- is not attempted; every range's Host still calls `Send`
independently and each `Send` still dials its own outbound connection per message, same
as `TCPTransport`. `kv.DurableRange` also does not use `MultiplexedTransport` yet --
`multiplex.go` is proven at the `raft.Host` layer only, the same
built-but-not-yet-wired-into-a-real-deployment-binary pattern this project has hit
several times before it closed it. Nothing durable is wired into a real binary yet
either -- `cmd/consensa` still only runs a single `DurableNode`, not a routed, multi-range
deployment. Replica movement and dynamic range policy also remain later work.

## What can fail?

Clients can use a stale descriptor transiently; the explicit mismatch and refresh path is
the recovery contract. Overlapping descriptors are rejected on creation. `DurableRange`
shares one `storage.Engine` between range data and `Persister`'s own bookkeeping (hard
state, log entries, snapshots), which live under a reserved `"raft/"` key prefix in the
same engine -- `Put`/`Delete` reject a user key with that prefix outright rather than
silently letting it corrupt Raft's own durable state.
