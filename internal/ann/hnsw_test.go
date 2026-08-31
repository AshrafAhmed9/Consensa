package ann

import (
	"github.com/ashraf/consensa/internal/vector"
	"testing"
)

// TestHNSWFindsNearest verifies the graph returns the nearest member on a deterministic toy set.
func TestHNSWFindsNearest(t *testing.T) {
	h, e := NewHNSW(Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1})
	if e != nil {
		t.Fatal(e)
	}
	for id, v := range map[string]vector.Vector{"a": {0, 0}, "b": {5, 5}, "c": {1, 1}} {
		if e := h.Insert(id, v); e != nil {
			t.Fatal(e)
		}
	}
	got, e := h.Search(vector.Vector{.9, .9}, 1, 0)
	if e != nil || len(got) != 1 || got[0].ID != "c" {
		t.Fatalf("search=%v,%v", got, e)
	}
}

// TestRepairDropsCrossBoundaryNodes proves a child index cannot retain an edge to a parent key.
func TestRepairDropsCrossBoundaryNodes(t *testing.T) {
	h, _ := NewHNSW(Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1})
	_ = h.Insert("a", vector.Vector{0, 0})
	_ = h.Insert("z", vector.Vector{2, 2})
	h.Repair(func(id string) bool { return id == "a" })
	if len(h.nodes) != 1 {
		t.Fatalf("nodes after repair = %d", len(h.nodes))
	}
	for _, ids := range h.nodes["a"].neighbors {
		for _, id := range ids {
			if id == "z" {
				t.Fatal("cross-boundary edge survived")
			}
		}
	}
}

// TestInsertNeverCreatesSelfLoop guards the publish-before-trim ordering used by insert.
func TestInsertNeverCreatesSelfLoop(t *testing.T) {
	h, _ := NewHNSW(Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1})
	_ = h.Insert("a", vector.Vector{0, 0})
	_ = h.Insert("b", vector.Vector{1, 1})
	for id, n := range h.nodes {
		for _, ids := range n.neighbors {
			for _, neighbor := range ids {
				if id == neighbor {
					t.Fatalf("self loop on %q", id)
				}
			}
		}
	}
}
