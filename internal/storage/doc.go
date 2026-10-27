// Package storage is a tiny on-disk key-value store, used as Raft's durable log.
//
// Every write is appended to a log file (the write-ahead log, or WAL) with a checksum, and
// also stored in an in-memory map that serves reads. When the store is opened again, the
// WAL is replayed to rebuild the map. There are no other files and nothing is ever
// compacted, so the WAL only grows. That is fine for this project and keeps the code small.
package storage
