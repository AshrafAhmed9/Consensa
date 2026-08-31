# Phase 1: storage

## Why does this exist?

Raft needs a durable ordered log and later vector metadata needs durable values. The LSM
engine provides that foundation without depending on an external database.

## How does it work?

Each mutation is appended to a length-prefixed, CRC32-protected WAL before entering an
ordered skiplist memtable. Flushing writes a sorted immutable SSTable. Reads examine the
memtable and newest tables first; MVCC keys reverse-encode timestamps so newer versions
sort before older versions of the same user key.

## What alternatives existed?

A B-tree, a standard map plus sort-on-flush, and leveled compaction would all work.
Skiplist traversal exposes the ordering property directly, while size-tiered merging is
the smallest compaction policy worth measuring.

## What tradeoff was made?

The initial SSTable format is intentionally simple and keeps records resident after open.
It is correct and inspectable, but it is not yet the planned block-cache implementation.
That improvement must be driven by a benchmark, not speculation.

## What can fail?

`SyncEvery > 1` explicitly permits the newest un-synced writes to disappear on power
loss. A corrupt complete WAL record is reported rather than guessed through. There is no
concurrent-writer policy beyond the public mutex; Raft will define distributed ordering.
