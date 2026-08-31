package kv

import (
	"bytes"
	"errors"
	"sort"
)

// Meta stores descriptors in start-key order. It is intentionally a simple static catalog
// until meta ranges themselves are backed by Raft in the multi-node assembly.
type Meta struct{ descriptors []Descriptor }

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
	m.descriptors = next.descriptors
	return nil
}
