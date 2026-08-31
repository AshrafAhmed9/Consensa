package server

import (
	"context"
	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/ann"
	"github.com/ashraf/consensa/internal/vector"
	"testing"
)

// TestBatchGetReturnsOnlyKnownVectors ensures API reads do not manufacture absent records.
func TestBatchGetReturnsOnlyKnownVectors(t *testing.T) {
	index, _ := ann.NewHNSW(ann.Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1})
	s := NewService(index)
	s.vectors["a"] = vector.Vector{1, 2}
	got, e := s.BatchGet(context.Background(), &consensav1.BatchGetRequest{Ids: []string{"a", "missing"}})
	if e != nil || len(got.Vectors) != 1 || len(got.Vectors["a"].Values) != 2 {
		t.Fatalf("BatchGet=%#v,%v", got, e)
	}
}
