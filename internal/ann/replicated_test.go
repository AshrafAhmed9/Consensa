package ann

import (
	"bytes"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/vector"
	"testing"
)

// TestReplicatedIndexAppliesIdenticalGraphMutations proves Raft order produces byte-identical graphs.
func TestReplicatedIndexAppliesIdenticalGraphMutations(t *testing.T) {
	r, e := NewReplicatedIndex([]raft.NodeID{1, 2, 3}, Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1})
	if e != nil {
		t.Fatal(e)
	}
	for _, item := range []struct {
		id string
		v  vector.Vector
	}{{"a", vector.Vector{0, 0}}, {"b", vector.Vector{1, 1}}} {
		if e := r.Insert(item.id, item.v); e != nil {
			t.Fatal(e)
		}
	}
	a, _ := r.Snapshot(1)
	b, _ := r.Snapshot(2)
	c, _ := r.Snapshot(3)
	if !bytes.Equal(a, b) || !bytes.Equal(b, c) {
		t.Fatal("replica graph snapshots diverged")
	}
	if e := r.Delete("a"); e != nil {
		t.Fatal(e)
	}
	a, _ = r.Snapshot(1)
	b, _ = r.Snapshot(2)
	c, _ = r.Snapshot(3)
	if !bytes.Equal(a, b) || !bytes.Equal(b, c) {
		t.Fatal("replica deletion snapshots diverged")
	}
}
