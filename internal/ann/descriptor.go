package ann

import (
	"errors"

	"github.com/ashraf/consensa/internal/raft"
)

// Descriptor assigns a half-open span of vector IDs, ordered lexicographically as Go
// strings, to a fixed replica set. This mirrors kv.Descriptor's shape exactly (see
// internal/kv/descriptor.go), but partitions the ID space directly rather than an
// arbitrary byte-key space, since a vector's ID is already the only stable identifier
// ExecuteLiveSplit (below) and Service (internal/server) have to route on.
type Descriptor struct {
	ID         uint64
	Start, End string
	Replicas   []raft.NodeID
}

// Contains reports whether id belongs to [Start, End). An empty End is positive infinity.
func (d Descriptor) Contains(id string) bool {
	return id >= d.Start && (d.End == "" || id < d.End)
}

// ErrRangeKeyMismatch asks the client/router to refresh stale metadata -- the vector-plane
// counterpart of kv.ErrRangeKeyMismatch.
var ErrRangeKeyMismatch = errors.New("ann: range key mismatch")

// SplitDescriptor divides one descriptor at splitKey without a gap or overlap, identical
// in structure to kv.SplitDescriptor. Callers persist the replacement atomically in
// metadata (Meta.Replace) before exposing either child to routing.
func SplitDescriptor(parent Descriptor, splitKey string, leftID, rightID uint64) (Descriptor, Descriptor, error) {
	if !parent.Contains(splitKey) || splitKey == parent.Start || (parent.End != "" && splitKey == parent.End) {
		return Descriptor{}, Descriptor{}, errors.New("ann: split key outside parent interior")
	}
	if leftID == 0 || rightID == 0 || leftID == rightID {
		return Descriptor{}, Descriptor{}, errors.New("ann: invalid child IDs")
	}
	left := Descriptor{ID: leftID, Start: parent.Start, End: splitKey, Replicas: append([]raft.NodeID(nil), parent.Replicas...)}
	right := Descriptor{ID: rightID, Start: splitKey, End: parent.End, Replicas: append([]raft.NodeID(nil), parent.Replicas...)}
	return left, right, nil
}
