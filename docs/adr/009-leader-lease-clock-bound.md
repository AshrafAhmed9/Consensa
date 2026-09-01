# ADR 009: leader leases require a bounded clock offset

## Status

Accepted for the lease API; the zero-assumption `ReadIndex` path remains the default for
linearizable KV reads.

## Context

Serving a read from a local leader role alone is unsafe after a partition: an old leader
can still believe it leads while a newer majority has elected another node. `ReadIndex`
solves that with a live quorum confirmation, but costs a round trip. A time-bounded lease
can avoid that round trip only if clocks have a known maximum disagreement.

## Decision

Let `max_offset` be the maximum absolute difference between any two healthy node clocks.
A leader may use a lease only after a quorum has acknowledged it, and only until its local
clock reaches `lease_expiration - max_offset`. A follower may use a published closed
timestamp only when its applied Raft index reaches the timestamp's accompanying index and
the requested timestamp is not newer than that promise.

If clock health is unavailable, the offset bound is exceeded, a lease is near expiry, or
leadership may have changed, the node must use `ReadIndex` instead. It must not silently
serve a lease read on the hope that clocks are close enough.

## Consequences

- Lease reads can be faster than quorum barriers when time synchronization is operationally
  trustworthy.
- The safety claim now depends on monitoring clock offset. A sufficiently bad clock can
  permit two nodes to believe incompatible lease intervals are valid.
- Tests must deliberately violate the bound and demonstrate that the code rejects the
  lease path; this is not a failure mode a happy-path benchmark can establish.
- **Update: live lease grant replication, closed-timestamp advancement, and an actual
  follower-read path are now implemented and proven against a real 3-node group** --
  `DurableRange.GrantLease`/`AdvanceClosedTimestamp`/`FollowerRead`
  (`internal/kv/durable_range.go`), `TestFollowerReadServesOnceLeasedAndClosed`. What
  remains genuinely unfinished: lease *revocation* on leadership change (a deposed
  leader's granted lease simply expires on its own clock rather than being actively
  invalidated), a real production policy for how often to advance the closed timestamp
  (currently the caller's choice, uncalled by any running binary), and a client-facing
  RPC surface for follower reads. The admission predicates and `ReadIndex` fallback this
  ADR originally described are unchanged.
