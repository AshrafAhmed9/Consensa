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

// TestBatchGetUsesIndexPayload proves exact ID reads do not depend on Service.vectors,
// which is intentionally process-local and empty after a gRPC service restart.
func TestBatchGetUsesIndexPayload(t *testing.T) {
	index, err := ann.NewHNSW(ann.Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Insert("durable", vector.Vector{3, 4}); err != nil {
		t.Fatal(err)
	}
	service := NewService(index)
	got, err := service.BatchGet(context.Background(), &consensav1.BatchGetRequest{Ids: []string{"durable"}})
	if err != nil || got.Vectors["durable"] == nil || len(got.Vectors["durable"].Values) != 2 {
		t.Fatalf("BatchGet exact index value = %#v, %v", got, err)
	}
}

// TestRequestCountIncrementsAcrossRPCs proves RequestCount (cmd/consensa's source for the
// consensa_range_qps metric) actually reflects real traffic across every data-plane RPC,
// not just one -- a counter that only some handlers touched would silently understate QPS.
func TestRequestCountIncrementsAcrossRPCs(t *testing.T) {
	index, _ := ann.NewHNSW(ann.Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1})
	s := NewService(index)
	if got := s.RequestCount(); got != 0 {
		t.Fatalf("RequestCount before any RPC = %d, want 0", got)
	}

	if _, err := s.BatchGet(context.Background(), &consensav1.BatchGetRequest{Ids: []string{"missing"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Delete(context.Background(), &consensav1.DeleteRequest{Id: "absent"}); err == nil {
		t.Fatal("expected Delete of an absent ID to fail")
	}
	// Delete still counts even though it failed -- RequestCount measures load received,
	// not requests that succeeded, matching what a real QPS metric should mean.
	if got := s.RequestCount(); got != 2 {
		t.Fatalf("RequestCount after BatchGet+Delete = %d, want 2", got)
	}
}
