// Package ann contains deterministic approximate-nearest-neighbour indexes. HNSW graph
// mutations are represented explicitly (see Mutation in persist.go) so Raft can replicate
// identical graph topology across replicas: given the same mutation sequence and the same
// Config.Seed, every replica's level assignment and neighbour selection is bit-identical.
//
// Two compositions of that index exist. ReplicatedIndex (replicated.go) drives HNSW
// mutations through the in-memory raft.Cluster test harness; it is deliberately not durable
// or networked, and exists to prove graph replicas stay identical under Raft-ordered
// mutation without the cost of real I/O. DurableNode (durable.go) drives the same HNSW type
// through a real raft.Host over real TCP, backed by an on-disk storage.Engine -- it is
// what actually survives a process restart, recovering its graph by replaying its own
// persisted Raft log rather than by any HNSW-specific snapshot mechanism.
//
// This package does not own sharding policy (which range a vector belongs to) -- that is
// internal/kv's responsibility once ranges exist.
package ann
