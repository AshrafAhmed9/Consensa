package server

import (
	"context"
	"io"
	"testing"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/ann"
	"github.com/ashraf/consensa/internal/raft"
)

// TestServiceCanUseReplicatedIndex proves the public write path can target committed graph mutations.
func TestServiceCanUseReplicatedIndex(t *testing.T) {
	index, err := ann.NewReplicatedIndex([]raft.NodeID{1, 2, 3}, ann.Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(index)
	stream := &upsertStream{requests: []*consensav1.UpsertRequest{{Id: "a", Vector: &consensav1.Vector{Values: []float32{1, 1}}}}}
	if err := service.Upsert(stream); err != nil {
		t.Fatal(err)
	}
	got, err := service.BatchGet(context.Background(), &consensav1.BatchGetRequest{Ids: []string{"a"}})
	if err != nil || got.Vectors["a"] == nil {
		t.Fatalf("BatchGet=%v,%v", got, err)
	}
	status, err := service.Status(context.Background(), &consensav1.StatusRequest{})
	if err != nil || status.Role != "leader" || status.Term == 0 {
		t.Fatalf("Status=%#v,%v", status, err)
	}
}

// TestUpsertRejectsWholeMalformedBatch proves a later invalid vector cannot partially ingest
// an earlier vector from the same client stream.
func TestUpsertRejectsWholeMalformedBatch(t *testing.T) {
	index, err := ann.NewHNSW(ann.Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(index)
	stream := &upsertStream{requests: []*consensav1.UpsertRequest{{Id: "valid", Vector: &consensav1.Vector{Values: []float32{1, 1}}}, {Id: "invalid", Vector: &consensav1.Vector{Values: []float32{1}}}}}
	if err := service.Upsert(stream); err == nil {
		t.Fatal("malformed batch accepted")
	}
	got, err := service.BatchGet(context.Background(), &consensav1.BatchGetRequest{Ids: []string{"valid"}})
	if err != nil || len(got.Vectors) != 0 {
		t.Fatalf("partial batch visible: %#v, %v", got, err)
	}
}

type upsertStream struct {
	consensav1.Consensa_UpsertServer
	requests []*consensav1.UpsertRequest
	position int
}

func (s *upsertStream) Recv() (*consensav1.UpsertRequest, error) {
	if s.position >= len(s.requests) {
		return nil, io.EOF
	}
	request := s.requests[s.position]
	s.position++
	return request, nil
}

func (s *upsertStream) SendAndClose(*consensav1.UpsertResponse) error { return nil }
