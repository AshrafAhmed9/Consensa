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
