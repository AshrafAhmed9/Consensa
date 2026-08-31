package kv

import (
	"bytes"
	"errors"

	"github.com/ashraf/consensa/internal/raft"
)

// MergeDescriptors combines adjacent ranges with identical replica placement. The metadata
// transaction that publishes the result must first establish the coordinated Raft barrier;
// this function only protects the range-ownership invariant at that boundary.
func MergeDescriptors(left, right Descriptor, mergedID uint64) (Descriptor, error) {
	if mergedID == 0 || !bytes.Equal(left.End, right.Start) {
		return Descriptor{}, errors.New("kv: ranges are not adjacent")
	}
	if len(left.Replicas) != len(right.Replicas) {
		return Descriptor{}, errors.New("kv: replica sets differ")
	}
	for i, replica := range left.Replicas {
		if replica != right.Replicas[i] {
			return Descriptor{}, errors.New("kv: replica sets differ")
		}
	}
	return Descriptor{ID: mergedID, Start: append([]byte(nil), left.Start...), End: append([]byte(nil), right.End...), Replicas: append([]raft.NodeID(nil), left.Replicas...)}, nil
}
