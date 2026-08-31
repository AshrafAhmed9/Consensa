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

This is still an in-memory assembly, not a live node implementation: shared socket
transport, durable per-range storage, cross-process heartbeat batching, replica movement,
and dynamic range policy remain later work.

## What can fail?

Clients can use a stale descriptor transiently; the explicit mismatch and refresh path is
the recovery contract. Overlapping descriptors are rejected on creation.
