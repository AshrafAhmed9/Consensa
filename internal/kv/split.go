package kv

import (
	"bytes"
	"errors"

	"github.com/ashraf/consensa/internal/raft"
)

// SplitDescriptor divides one descriptor at splitKey without a gap or overlap. Callers
// persist the replacement atomically in metadata before exposing either child to routing.
func SplitDescriptor(parent Descriptor, splitKey []byte, leftID, rightID uint64) (Descriptor, Descriptor, error) {
	if !parent.Contains(splitKey) || bytes.Equal(splitKey, parent.Start) || (len(parent.End) > 0 && bytes.Equal(splitKey, parent.End)) {
		return Descriptor{}, Descriptor{}, errors.New("kv: split key outside parent interior")
	}
	if leftID == 0 || rightID == 0 || leftID == rightID {
		return Descriptor{}, Descriptor{}, errors.New("kv: invalid child IDs")
	}
	left := Descriptor{ID: leftID, Start: append([]byte(nil), parent.Start...), End: append([]byte(nil), splitKey...), Replicas: append([]raft.NodeID(nil), parent.Replicas...)}
	right := Descriptor{ID: rightID, Start: append([]byte(nil), splitKey...), End: append([]byte(nil), parent.End...), Replicas: append([]raft.NodeID(nil), parent.Replicas...)}
	return left, right, nil
}
