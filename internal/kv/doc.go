// Package kv maps keys to independently replicated ranges. Metadata is represented as
// ordinary descriptors so routing can be tested separately from Raft; dynamic split and
// merge policy are deliberately deferred to Phase 12.
package kv
