package kv

import (
	"testing"

	"github.com/ashraf/consensa/internal/raft"
)

// TestSplitDescriptorPreservesExactOwnership proves every sample key remains in exactly one child.
func TestSplitDescriptorPreservesExactOwnership(t *testing.T) {
	p := Descriptor{ID: 1, Start: []byte("a"), End: []byte("z"), Replicas: []raft.NodeID{1, 2, 3}}
	l, r, err := SplitDescriptor(p, []byte("m"), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{[]byte("a"), []byte("l"), []byte("m"), []byte("y")} {
		if l.Contains(key) == r.Contains(key) {
			t.Fatalf("ownership broken for %q", key)
		}
	}
}

// TestMetaReplacePublishesSplitAtomically proves lookups resolve through a complete child catalog.
func TestMetaReplacePublishesSplitAtomically(t *testing.T) {
	m, err := NewMeta([]Descriptor{{ID: 1, Start: nil, End: nil, Replicas: []raft.NodeID{1, 2, 3}}})
	if err != nil {
		t.Fatal(err)
	}
	parent, _ := m.Lookup([]byte("m"))
	left, right, err := SplitDescriptor(parent, []byte("m"), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Replace([]Descriptor{left, right}); err != nil {
		t.Fatal(err)
	}
	if got, err := m.Lookup([]byte("a")); err != nil || got.ID != 2 {
		t.Fatalf("left=%v,%v", got, err)
	}
	if got, err := m.Lookup([]byte("z")); err != nil || got.ID != 3 {
		t.Fatalf("right=%v,%v", got, err)
	}
}
