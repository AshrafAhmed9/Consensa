# ADR 005: Raft-log graph mutations

## Decision

HNSW insertions are encoded as deterministic mutations and applied in Raft order. Snapshots
serialize the resulting graph in a canonical order.

## Rationale

Rebuilding a graph from stored vectors would reduce the Raft log but would require proving
that randomized insertion produces identical topology on every replica. Logging mutations
makes graph identity explicit and is easier to test with byte-for-byte snapshots. The cost
is a larger log; Raft snapshots bound it.
