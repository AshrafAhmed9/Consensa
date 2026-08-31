package kv

import (
	"github.com/ashraf/consensa/internal/raft"
	"testing"
)

// TestMergeDescriptorsPreservesCombinedOwnership proves a merge restores exactly the two child spans.
func TestMergeDescriptorsPreservesCombinedOwnership(t *testing.T) {
	replicas := []raft.NodeID{1, 2, 3}
	left := Descriptor{ID: 1, Start: []byte("a"), End: []byte("m"), Replicas: replicas}
	right := Descriptor{ID: 2, Start: []byte("m"), End: []byte("z"), Replicas: replicas}
	merged, e := MergeDescriptors(left, right, 3)
	if e != nil {
		t.Fatal(e)
	}
	for _, key := range [][]byte{[]byte("a"), []byte("m"), []byte("y")} {
		if !merged.Contains(key) {
			t.Fatalf("merged range lost %q", key)
		}
	}
}
