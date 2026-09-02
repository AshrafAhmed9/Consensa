package ann

import (
	"errors"
	"sort"
	"sync"
)

// Meta stores vector-range descriptors in start-ID order -- the vector-plane counterpart
// of kv.Meta (internal/kv/meta.go), same synchronization reasoning: Lookup is called from
// concurrent gRPC handler goroutines (server.Service) while Replace can run live from a
// split's background goroutine, so the descriptor slice needs a real lock, not just
// bootstrap-then-read discipline.
type Meta struct {
	mu          sync.RWMutex
	descriptors []Descriptor
}

// NewMeta validates that descriptors leave neither overlap nor ambiguous routing.
func NewMeta(ds []Descriptor) (*Meta, error) {
	out := append([]Descriptor(nil), ds...)
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	for i, d := range out {
		if d.ID == 0 || len(d.Replicas) == 0 {
			return nil, errors.New("ann: descriptor ID and replicas required")
		}
		if d.End != "" && d.Start >= d.End {
			return nil, errors.New("ann: invalid range bounds")
		}
		if i > 0 && out[i-1].End != "" && d.Start < out[i-1].End {
			return nil, errors.New("ann: overlapping descriptors")
		}
	}
	return &Meta{descriptors: out}, nil
}

// Lookup returns the one descriptor responsible for id.
func (m *Meta) Lookup(id string) (Descriptor, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, d := range m.descriptors {
		if d.Contains(id) {
			return d, nil
		}
	}
	return Descriptor{}, ErrRangeKeyMismatch
}

// Replace atomically validates and installs a complete descriptor catalog.
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

// All returns a copy of the current descriptor catalog, in start-ID order -- used by
// ExecuteLiveSplit's caller to build the next catalog (every existing descriptor, with
// the one being split replaced by its two children).
func (m *Meta) All() []Descriptor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]Descriptor(nil), m.descriptors...)
}
