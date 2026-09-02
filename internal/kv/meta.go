package kv

import (
	"bytes"
	"errors"
	"sort"
	"sync"
)

// Meta stores descriptors in start-key order. It is intentionally a simple static catalog
// until meta ranges themselves are backed by Raft in the multi-node assembly.
//
// mu guards descriptors: Lookup was originally only ever called after every Replace had
// already happened (bootstrap-then-read), but a live split (kv.ExecuteLiveSplit) now
// calls Replace from a background goroutine while gRPC handler goroutines concurrently
// call Lookup through Router.Route -- an unsynchronized slice-header reassignment
// racing a concurrent range-over read is a real data race under Go's memory model, not
// just a theoretical one, so this is no longer optional once Replace can run live.
type Meta struct {
	mu          sync.RWMutex
	descriptors []Descriptor
}

// NewMeta validates that descriptors leave neither overlap nor ambiguous routing.
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

// Lookup returns the one descriptor responsible for key.
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

// Replace atomically validates and installs a complete descriptor catalog. Split/merge
// coordinators construct the next catalog first, so routing observes either the old valid
// topology or the new valid topology—never an overlap or an ownership gap in between.
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

// All returns a copy of the current descriptor catalog, in start-key order -- used by
// kv.ExecuteLiveSplit (split.go) to build the next catalog (every existing descriptor,
// with the one being split replaced by its two children) without letting a caller hold a
// reference into Meta's internal slice across a concurrent Replace.
func (m *Meta) All() []Descriptor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]Descriptor(nil), m.descriptors...)
}
