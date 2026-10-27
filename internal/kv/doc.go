// Package kv holds the key-range ownership logic that specs/split.tla models: a
// Descriptor owns a range of keys, Meta is the catalog of ranges, and SplitDescriptor
// cuts one range in two with no gap and no overlap. It is plain logic with no Raft or
// I/O, and the server does not shard on it yet.
package kv
