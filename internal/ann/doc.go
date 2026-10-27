// Package ann contains the HNSW approximate-nearest-neighbour index. Graph mutations are
// explicit (see Mutation in persist.go) so Raft can replicate identical graph topology:
// given the same mutation sequence and the same Config.Seed, every replica's level
// assignment and neighbour selection is bit-identical.
//
// DurableNode (durable.go) drives an HNSW through a raft.Host over TCP, backed by an
// on-disk storage.Engine. A restarted node recovers its graph by replaying its own
// persisted Raft log.
package ann
