// Package kv maps keys to independently replicated ranges. Metadata is represented as
// ordinary descriptors (Descriptor, Meta, Router) so routing can be tested separately from
// Raft; dynamic split and merge *policy* (deciding when and where to split) is deliberately
// deferred to Phase 12 -- SplitDescriptor and MergeDescriptors here are the pure mechanics
// a policy will eventually call, not the policy itself.
//
// Two range state machines exist, both driven by the same byte-KV command wire format
// (rangeCommand in multiraft.go, shared through decodeRangeCommand): MultiRaft/rangeState
// apply commands into an in-memory map over raft.Cluster, the deterministic in-process
// harness used to prove routing and multi-group ticking without real I/O. DurableRange
// (durable_range.go) applies the identical commands as real MVCC writes into a real
// storage.Engine over a real raft.Host/TCP connection -- the durable counterpart, proven by
// a real crash-and-restart test rather than by replaying a log into memory (unlike
// internal/ann.DurableNode, a byte KV range has no in-memory structure that needs
// rebuilding, so storage.Engine's own recovery is sufficient on its own).
//
// One real Raft transport per range (one TCP listener, one storage directory) is what
// DurableRange gives you today. The plan's actual multi-range design -- many ranges
// multiplexed over one shared transport with batched cross-range heartbeats -- is a
// separate, not-yet-built piece of engineering; see docs/notes for the current phase.
package kv
