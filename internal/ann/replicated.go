package ann

import (
	"errors"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/vector"
)

// ReplicatedIndex applies HNSW mutations only after a deterministic Raft group commits
// them. It is the in-memory composition used to prove graph replicas stay bit-identical;
// socket transport and durable recovery remain responsibilities of the node assembly.
type ReplicatedIndex struct {
	cluster  *raft.Cluster
	replicas map[raft.NodeID]*HNSW
}

// NewReplicatedIndex creates one graph per Raft replica and drives an initial election.
func NewReplicatedIndex(ids []raft.NodeID, cfg Config) (*ReplicatedIndex, error) {
	c, err := raft.NewCluster(ids)
	if err != nil {
		return nil, err
	}
	r := &ReplicatedIndex{cluster: c, replicas: map[raft.NodeID]*HNSW{}}
	for _, id := range ids {
		index, e := NewHNSW(cfg)
		if e != nil {
			return nil, e
		}
		r.replicas[id] = index
	}
	for i := 0; i < 3; i++ {
		if e := c.Tick(); e != nil {
			return nil, e
		}
	}
	return r, nil
}

// Insert commits an encoded mutation through the elected leader before applying it to every graph.
func (r *ReplicatedIndex) Insert(id string, v vector.Vector) error {
	data, err := EncodeMutation(id, v)
	if err != nil {
		return err
	}
	return r.commit(data)
}

// Delete commits a deterministic deletion before removing it from every graph replica.
func (r *ReplicatedIndex) Delete(id string) error {
	data, err := EncodeDeleteMutation(id)
	if err != nil {
		return err
	}
	return r.commit(data)
}

// Search serves from the current leader's graph. In this deterministic assembly all graphs
// are equal after committed mutations; choosing the leader preserves the normal read path.
func (r *ReplicatedIndex) Search(query vector.Vector, k, ef int) ([]Result, error) {
	leader, ok := r.cluster.Leader()
	if !ok {
		return nil, errors.New("ann: no Raft leader")
	}
	return r.replicas[leader].Search(query, k, ef)
}

// Validate checks a vector against the replicated collection's shared configuration.
func (r *ReplicatedIndex) Validate(v vector.Vector) error {
	leader, ok := r.cluster.Leader()
	if !ok {
		return errors.New("ann: no Raft leader")
	}
	return r.replicas[leader].Validate(v)
}

// Status exposes Raft leadership for an administrative API without exposing mutable nodes.
func (r *ReplicatedIndex) Status() (raft.NodeID, uint64, bool) { return r.cluster.Status() }

func (r *ReplicatedIndex) commit(data []byte) error {
	leader, ok := r.cluster.Leader()
	if !ok {
		return errors.New("ann: no Raft leader")
	}
	before := len(r.cluster.Applied(leader))
	if err := r.cluster.Propose(leader, data); err != nil {
		return err
	}
	for node, index := range r.replicas {
		applied := r.cluster.Applied(node)
		if len(applied) != before+1 {
			return errors.New("ann: mutation not committed on every replica")
		}
		if err := index.ApplyMutation(applied[before]); err != nil {
			return err
		}
	}
	return nil
}

// Snapshot returns a canonical replica graph snapshot for byte-identity checks.
func (r *ReplicatedIndex) Snapshot(id raft.NodeID) ([]byte, error) {
	index := r.replicas[id]
	if index == nil {
		return nil, errors.New("ann: unknown replica")
	}
	return index.Snapshot()
}
