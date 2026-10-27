package server

import (
	"context"
	"io"
	"sort"
	"sync"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/ann"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/vector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Index is the narrow vector mutation/query contract the service needs. ann.DurableNode
// implements it on top of Raft.
type Index interface {
	Insert(string, vector.Vector) error
	Delete(string) error
	Search(vector.Vector, int, int) ([]ann.Result, error)
	Validate(vector.Vector) error
}

// Service is the public gRPC API in front of one index.
type Service struct {
	consensav1.UnimplementedConsensaServer
	mu    sync.RWMutex
	index Index
}

// NewService creates an API service backed by index.
func NewService(index Index) *Service { return &Service{index: index} }

// Upsert accepts a streaming ingest batch and rejects malformed vectors before any of it is indexed.
func (s *Service) Upsert(stream consensav1.Consensa_UpsertServer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var requests []*consensav1.UpsertRequest
	seen := map[string]bool{}
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if r.Id == "" || r.Vector == nil || len(r.Vector.Values) == 0 {
			return status.Error(codes.InvalidArgument, "id and non-empty vector are required")
		}
		if seen[r.Id] {
			return status.Error(codes.InvalidArgument, "duplicate ID in upsert stream")
		}
		seen[r.Id] = true
		if err := s.index.Validate(vector.Vector(r.Vector.Values)); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		requests = append(requests, r)
	}
	for _, r := range requests {
		if err := s.index.Insert(r.Id, vector.Vector(r.Vector.Values)); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}
	return stream.SendAndClose(&consensav1.UpsertResponse{Accepted: uint64(len(requests))})
}

// Search streams the top-k nearest neighbours in ascending distance order.
func (s *Service) Search(r *consensav1.SearchRequest, stream consensav1.Consensa_SearchServer) error {
	if r.Query == nil || len(r.Query.Values) == 0 || r.K == 0 {
		return status.Error(codes.InvalidArgument, "query and positive k are required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	results, err := s.index.Search(vector.Vector(r.Query.Values), int(r.K), int(r.Ef))
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Distance < results[j].Distance })
	if int(r.K) < len(results) {
		results = results[:r.K]
	}
	for _, x := range results {
		if err := stream.Send(&consensav1.SearchResult{Id: x.ID, Distance: x.Distance}); err != nil {
			return err
		}
	}
	return nil
}

// Delete removes a vector and its graph links.
func (s *Service) Delete(_ context.Context, request *consensav1.DeleteRequest) (*consensav1.DeleteResponse, error) {
	if request.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.index.Delete(request.Id); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &consensav1.DeleteResponse{}, nil
}

// Status reports whether this node is the Raft leader, and its term, when the index is
// Raft-backed.
func (s *Service) Status(context.Context, *consensav1.StatusRequest) (*consensav1.StatusResponse, error) {
	replicated, ok := s.index.(interface {
		Status() (raft.NodeID, uint64, bool)
	})
	if !ok {
		return &consensav1.StatusResponse{Role: "single-node"}, nil
	}
	_, term, leader := replicated.Status()
	if leader {
		return &consensav1.StatusResponse{Role: "leader", Term: term}, nil
	}
	return &consensav1.StatusResponse{Role: "not-leader", Term: term}, nil
}
