// Package ann contains deterministic approximate-nearest-neighbour indexes. HNSW graph
// mutations are represented explicitly so Raft can replicate identical graph topology;
// this package does not own replication, persistence, or sharding policy.
package ann
