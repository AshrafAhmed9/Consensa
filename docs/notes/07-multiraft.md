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

**Update: outbound connection pooling closes the other half of that claim.** Every range's
`Send` used to dial its own fresh TCP connection per message, per destination -- exactly
the "1,000 ranges must not mean 1,000x heartbeat traffic" cost this file used to say was
untouched. Ranges sharing a destination node now share one persistent outbound
connection (`connFor`/`pooledConn` in `multiplex.go`), and the receiving side reads many
frames off one connection instead of accepting-and-closing per message.
`TestMultiplexedTransportPoolsOutboundConnections` proves it directly: 10 sends across
two ranges to one destination open exactly one outbound connection, not ten.

Reading multiple frames per connection surfaced two real bugs the single-frame design had
never exercised, both fixed rather than routed around: `readFrame` used to wrap its
argument in a brand-new `bufio.Reader` on every call, which silently drops bytes already
read ahead into a discarded reader's buffer when the same connection is read in a loop
(now takes a `*bufio.Reader` the caller owns and reuses); and dispatching a received frame
straight to its range's handler from the shared connection's one read goroutine created
real cross-range head-of-line blocking that a dedicated-connection-per-range design never
had -- caught as an actual intermittent test failure, not by inspection, and fixed by
giving each range its own bounded inbox and worker goroutine so the shared read loop only
enqueues and moves on. What this still does not do: coalesce multiple ranges' messages
into a single wire frame -- each message is still its own frame, just sent over an
already-open connection instead of a freshly dialed one.

**What still doesn't close, stated plainly:** `cmd/consensa` (`main.go`) already calls
`raft.ListenMultiplexed` for its one shared per-process listener, so this pooling change
applies directly to the real deployed binary, not just to `multiplex_test.go` -- but the
binary's own end-to-end test does not specifically assert connection reuse, only that the
vector and KV planes both work correctly over the shared listener; the reuse claim itself
is proven at the `raft.Host`-layer unit test (`TestMultiplexedTransportPoolsOutboundConnections`).
Replica movement and dynamic range policy remain later work.

## What can fail?

Clients can use a stale descriptor transiently; the explicit mismatch and refresh path is
the recovery contract. Overlapping descriptors are rejected on creation. `DurableRange`
shares one `storage.Engine` between range data and `Persister`'s own bookkeeping (hard
state, log entries, snapshots), which live under a reserved `"raft/"` key prefix in the
same engine -- `Put`/`Delete` reject a user key with that prefix outright rather than
silently letting it corrupt Raft's own durable state.
