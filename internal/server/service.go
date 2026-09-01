package server

import (
	"context"
	"errors"
	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/ann"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/vector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync"
	"sync/atomic"
)

// Index is the narrow vector mutation/query contract shared by a local and Raft-backed index.
type Index interface {
	Insert(string, vector.Vector) error
	Delete(string) error
	Search(vector.Vector, int, int) ([]ann.Result, error)
	Validate(vector.Vector) error
}

// Service is the public API implementation. Its index can be local or Raft-backed.
type Service struct {
	consensav1.UnimplementedConsensaServer
	mu           sync.RWMutex
	index        Index
	vectors      map[string]vector.Vector
	requestCount atomic.Uint64
}

// NewService creates an API service with a configured HNSW index.
func NewService(index Index) *Service {
	return &Service{index: index, vectors: map[string]vector.Vector{}}
}

// RequestCount returns the total number of data-plane requests (Search, Upsert, Delete,
// BatchGet) this service has handled since it started. It is a raw counter, not a rate --
// converting it into a QPS gauge means sampling the delta over a time window, which is
// the caller's job (see cmd/consensa/main.go's metrics tick loop) so this type stays free
// of a clock dependency it does not otherwise need.
func (s *Service) RequestCount() uint64 { return s.requestCount.Load() }

// Upsert accepts a streaming ingest batch and rejects malformed vectors before partial indexing.
func (s *Service) Upsert(stream consensav1.Consensa_UpsertServer) error {
	s.requestCount.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	var requests []*consensav1.UpsertRequest
	for {
		r, e := stream.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		if r.Id == "" || r.Vector == nil || len(r.Vector.Values) == 0 {
			return status.Error(codes.InvalidArgument, "id and non-empty vector are required")
		}
		requests = append(requests, r)
	}
	seen := map[string]bool{}
	for _, request := range requests {
		if seen[request.Id] {
			return status.Error(codes.InvalidArgument, "duplicate ID in upsert stream")
		}
		seen[request.Id] = true
		if err := s.index.Validate(vector.Vector(request.Vector.Values)); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}
	for _, request := range requests {
		v := vector.Vector(request.Vector.Values)
		if _, exists := s.vectors[request.Id]; exists {
			if err := s.index.Delete(request.Id); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
		}
		if err := s.index.Insert(request.Id, v); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		s.vectors[request.Id] = append(vector.Vector(nil), v...)
	}
	return stream.SendAndClose(&consensav1.UpsertResponse{Accepted: uint64(len(requests))})
}

// Search streams ordered ANN results so large result sets need not wait for response assembly.
func (s *Service) Search(r *consensav1.SearchRequest, stream consensav1.Consensa_SearchServer) error {
	s.requestCount.Add(1)
	if r.Query == nil || len(r.Query.Values) == 0 || r.K == 0 {
		return status.Error(codes.InvalidArgument, "query and positive k are required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	rs, e := s.index.Search(vector.Vector(r.Query.Values), int(r.K), int(r.Ef))
	if e != nil {
		return status.Error(codes.InvalidArgument, e.Error())
	}
	for _, x := range rs {
		if e := stream.Send(&consensav1.SearchResult{Id: x.ID, Distance: x.Distance}); e != nil {
			return e
		}
	}
	return nil
}

// Delete removes a vector and its graph links. Raft-backed deployments call this only after
// the corresponding replicated deletion mutation commits.
func (s *Service) Delete(_ context.Context, request *consensav1.DeleteRequest) (*consensav1.DeleteResponse, error) {
	s.requestCount.Add(1)
	if request.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.index.Delete(request.Id); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	delete(s.vectors, request.Id)
	return &consensav1.DeleteResponse{}, nil
}

// BatchGet returns vectors that remain visible in the local single-node index.
func (s *Service) BatchGet(_ context.Context, r *consensav1.BatchGetRequest) (*consensav1.BatchGetResponse, error) {
	s.requestCount.Add(1)
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := &consensav1.BatchGetResponse{Vectors: map[string]*consensav1.Vector{}}
	for _, id := range r.Ids {
		if v, ok := s.vectors[id]; ok {
			out.Vectors[id] = &consensav1.Vector{Values: v}
		}
	}
	return out, nil
}

// Status reports the intentionally narrow initial node state.
func (s *Service) Status(context.Context, *consensav1.StatusRequest) (*consensav1.StatusResponse, error) {
	if replicated, ok := s.index.(interface {
		Status() (raft.NodeID, uint64, bool)
	}); ok {
		_, term, leader := replicated.Status()
		if leader {
			return &consensav1.StatusResponse{Role: "leader", Term: term}, nil
		}
		return &consensav1.StatusResponse{Role: "candidate", Term: term}, nil
	}
	return &consensav1.StatusResponse{Role: "single-node", Term: 0}, nil
}

var _ = errors.New
