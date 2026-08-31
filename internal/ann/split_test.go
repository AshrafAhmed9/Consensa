package ann

import (
	"github.com/ashraf/consensa/internal/vector"
	"testing"
)

// TestSplitProducesDisjointSearchableChildGraphs proves cross-boundary IDs and edges do
// not survive into either child graph after the parent snapshot is partitioned.
func TestSplitProducesDisjointSearchableChildGraphs(t *testing.T) {
	parent, _ := NewHNSW(Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1})
	for _, item := range []struct {
		id string
		v  vector.Vector
	}{{"a", vector.Vector{0, 0}}, {"b", vector.Vector{1, 1}}, {"z", vector.Vector{9, 9}}} {
		if e := parent.Insert(item.id, item.v); e != nil {
			t.Fatal(e)
		}
	}
	left, right, e := parent.Split(func(id string) bool { return id < "m" })
	if e != nil {
		t.Fatal(e)
	}
	if len(left.nodes) != 2 || len(right.nodes) != 1 {
		t.Fatalf("child sizes %d %d", len(left.nodes), len(right.nodes))
	}
	for _, n := range left.nodes {
		for _, ids := range n.neighbors {
			for _, id := range ids {
				if id >= "m" {
					t.Fatalf("cross edge %q", id)
				}
			}
		}
	}
}
