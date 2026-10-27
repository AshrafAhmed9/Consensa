// Package storage is a small on-disk key-value store, used as Raft's durable log.
//
// It works like a simple LSM tree:
//  1. Each write is appended to a write-ahead log (the WAL, with a checksum per record)
//     and put in an in-memory sorted map (a skiplist).
//  2. When the in-memory map gets big, it is written out as an immutable sorted file
//     (an SSTable) and the WAL is started afresh.
//  3. When there are too many SSTables, they are merged into one.
//
// On restart the SSTables are loaded and the WAL is replayed. There is one writer and no
// replication here; Raft above it decides the order of writes.
package storage
