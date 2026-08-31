// Package storage implements Consensa's durable, ordered single-node key-value engine.
// Writes first enter a CRC-protected WAL and an in-memory skiplist; immutable SSTables
// make recovery and reads simple. This package deliberately has one writer and no
// replication: Raft will provide ordering and concurrency control above it.
package storage
