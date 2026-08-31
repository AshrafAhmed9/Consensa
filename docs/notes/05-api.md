# Phase 5: gRPC API

## Why does this exist?

The gRPC surface lets bulk-ingest and retrieval clients use Consensa without depending on
the internal storage, Raft, or index packages.

## How does it work?

Upsert is client-streaming because ingest is batch-oriented. Search is server-streaming so
large result sets can flow incrementally. The service validates IDs, dimensions, and `k`
at the boundary, returning gRPC status errors instead of panics.

## What alternatives existed?

HTTP JSON is easier to inspect but less natural for high-volume typed vector payloads.
Hand-written RPC framing would duplicate commodity protocol work and distract from the
data-plane implementation.

## What tradeoff was made?

The initial service is single-node and uses the in-process index. The public contract is
stable while later phases wire mutations through Raft before acknowledging them.

## What can fail?

Deletion and replacement must not pretend to be durable until graph removal is encoded as
a replicated mutation. They return explicit `Unimplemented` errors for now.
