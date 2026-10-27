// Package kv holds the key-range ownership logic that split.tla models: a Descriptor owns
// a half-open key span, Meta is the catalog of descriptors, and SplitDescriptor divides one
// descriptor in two with no gap and no overlap. It is pure logic with no Raft or I/O, and
// the server does not shard on it yet.
package kv
