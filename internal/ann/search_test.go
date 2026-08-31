package ann

import (
	"github.com/ashraf/consensa/internal/vector"
	"testing"
)

// TestSearchUsesGraphTraversal ensures search can reach a neighbour through graph edges
// without consulting every stored vector as a brute-force fallback.
func TestSearchUsesGraphTraversal(t *testing.T) {
	h, _ := NewHNSW(Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 4, Seed: 1})
	for _, pair := range []struct {
		id string
		v  vector.Vector
	}{{"a", vector.Vector{0, 0}}, {"b", vector.Vector{1, 1}}, {"c", vector.Vector{2, 2}}} {
		if e := h.Insert(pair.id, pair.v); e != nil {
			t.Fatal(e)
		}
	}
	got, e := h.Search(vector.Vector{1.1, 1.1}, 1, 4)
	if e != nil || len(got) != 1 || got[0].ID != "b" {
		t.Fatalf("search=%v,%v", got, e)
	}
}
