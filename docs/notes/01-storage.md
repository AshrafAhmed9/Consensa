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

**A real finding, not a design decision: `DB.Get` never used the per-SSTable index or
Bloom filter this package built for it.** `sstable.get` (an O(log n) binary search over
that table's sorted records, gated by a Bloom-filter `mayContain` pre-check to skip
tables that can't hold the key at all) had zero callers anywhere -- `DB.Get`
(`engine.go`) does a full linear scan over every record from every memtable and SSTable
via `allRecordsNewest()` for every read, regardless of key. Found by actually running
`golangci-lint`'s `unused` check for the first time (see the CI-fix commit this session
that discovered CI itself had never once run to completion), not by design review. The
dead `sstable.get`, `skiplist.get`, and the entire Bloom filter (`bloom.go`) were removed
rather than kept as unreachable code pretending to be an optimization -- point-lookup
acceleration is real future work, not something partially built and forgotten under a
green checkmark. `DB.Get`'s current O(records) behavior across every SSTable and the
memtable is the actual, honest characteristic of this engine's read path today.

## What can fail?

`SyncEvery > 1` explicitly permits the newest un-synced writes to disappear on power
loss. A corrupt complete WAL record is reported rather than guessed through. There is no
concurrent-writer policy beyond the public mutex; Raft will define distributed ordering.
