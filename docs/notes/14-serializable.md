# Phase 14: serializability foundation

## Why does this exist?

Snapshot isolation allows write skew: transactions can read overlapping facts then write
different keys without directly conflicting.

## How does it work?

Each range tracks the highest read timestamp for a key. A write below it is pushed forward,
forcing validation/refresh rather than allowing the read-write edge to form a cycle.

## What alternatives existed?

Two-phase locking prevents anomalies but adds locks and deadlock handling. Timestamp
ordering preserves the optimistic transaction model used by the rest of this project.

## What tradeoff was made?

The cache is a primitive only; full serializable execution still needs it integrated with
intent writes and read refresh in the replicated coordinator.

## What can fail?

A cache lost on restart must be restored conservatively or accompanied by a timestamp
floor. An isolated cache test does not prove end-to-end serializability.
