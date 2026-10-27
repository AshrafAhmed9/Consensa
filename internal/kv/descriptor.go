package kv

import (
	"bytes"
	"errors"
	"sort"
	"sync"

	"github.com/ashraf/consensa/internal/raft"
)

// ErrRangeKeyMismatch means no range owns the key, so the caller's view is out of date.
var ErrRangeKeyMismatch = errors.New("kv: range key mismatch")

// Descriptor says which nodes hold one range of keys. The range is half-open:
// Start is included and End is not. An empty End means "to the end of the keyspace".
type Descriptor struct {
	ID         uint64
	Start, End []byte
	Replicas   []raft.NodeID
}

// Contains reports whether key falls inside the descriptor's range.
func (d Descriptor) Contains(key []byte) bool {
	return bytes.Compare(key, d.Start) >= 0 && (len(d.End) == 0 || bytes.Compare(key, d.End) < 0)
}

// Meta is the catalog of all ranges. Every key belongs to exactly one of them, which is
// the property specs/split.tla checks.
type Meta struct {
	mu          sync.RWMutex
	descriptors []Descriptor // sorted by Start
}

// NewMeta builds a catalog and rejects descriptors that are invalid or overlap.
func NewMeta(ds []Descriptor) (*Meta, error) {
	out := append([]Descriptor(nil), ds...)
	sort.Slice(out, func(i, j int) bool { return string(out[i].Start) < string(out[j].Start) })
	for i, d := range out {
		if d.ID == 0 || len(d.Replicas) == 0 {
			return nil, errors.New("kv: descriptor ID and replicas required")
		}
		if len(d.End) > 0 && bytes.Compare(d.Start, d.End) >= 0 {
			return nil, errors.New("kv: invalid range bounds")
		}
		if i > 0 && len(out[i-1].End) > 0 && bytes.Compare(d.Start, out[i-1].End) < 0 {
			return nil, errors.New("kv: overlapping descriptors")
		}
	}
	return &Meta{descriptors: out}, nil
}

// Lookup returns the range that owns key.
func (m *Meta) Lookup(key []byte) (Descriptor, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, d := range m.descriptors {
		if d.Contains(key) {
			return d, nil
		}
	}
	return Descriptor{}, ErrRangeKeyMismatch
}

// Replace swaps in a whole new catalog in one step, after checking it. Lookups see either
// the old catalog or the new one, never a mix with a gap or overlap.
func (m *Meta) Replace(descriptors []Descriptor) error {
	next, err := NewMeta(descriptors)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.descriptors = next.descriptors
	m.mu.Unlock()
	return nil
}

// SplitDescriptor cuts one range into two at splitKey: [Start, splitKey) and
// [splitKey, End). Together they cover exactly what the parent covered. The split key
// must be strictly inside the parent's range.
func SplitDescriptor(parent Descriptor, splitKey []byte, leftID, rightID uint64) (Descriptor, Descriptor, error) {
	if !parent.Contains(splitKey) || bytes.Equal(splitKey, parent.Start) {
		return Descriptor{}, Descriptor{}, errors.New("kv: split key outside parent interior")
	}
	if leftID == 0 || rightID == 0 || leftID == rightID {
		return Descriptor{}, Descriptor{}, errors.New("kv: invalid child IDs")
	}
	left := Descriptor{ID: leftID, Start: clone(parent.Start), End: clone(splitKey), Replicas: append([]raft.NodeID(nil), parent.Replicas...)}
	right := Descriptor{ID: rightID, Start: clone(splitKey), End: clone(parent.End), Replicas: append([]raft.NodeID(nil), parent.Replicas...)}
	return left, right, nil
}

func clone(b []byte) []byte { return append([]byte(nil), b...) }
