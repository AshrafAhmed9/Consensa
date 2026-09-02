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
	"sort"
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

// vectorGetter is optional because simple test indexes and older adapters may not retain
// exact payloads. DurableNode implements it, letting BatchGet survive a service-process
// restart instead of treating the process-local vectors map as the source of truth.
type vectorGetter interface {
	GetVector(string) (vector.Vector, bool)
}

// Service is the public API implementation. Each registered index can be local or
// Raft-backed, and a live split (cmd/consensa.executeAnnSplitIfRecommended) can register
// new ones at runtime -- indicesMu guards indices the same way KVService.storesMu guards
// its own range map (internal/server/kv_service.go), for the identical reason: Upsert/
// Search/Delete/BatchGet read it from concurrent gRPC handler goroutines while a split's
// background goroutine can call RegisterIndex live.
type Service struct {
	consensav1.UnimplementedConsensaServer
	mu           sync.RWMutex
	meta         *ann.Meta
	indicesMu    sync.RWMutex
	indices      map[uint64]Index
	vectors      map[string]vector.Vector
	requestCount atomic.Uint64
}

// NewService creates an API service with a single configured HNSW index, spanning the
// entire ID space as range 1 -- the same single-range starting shape kv.NewMeta's two
// static KV ranges use, so every existing caller (tests, cmd/consensa before a split ever
// runs) keeps working unchanged. A live split later replaces this one-descriptor catalog
// via meta.Replace and adds the fresh children via RegisterIndex.
func NewService(index Index) *Service {
	meta, err := ann.NewMeta([]ann.Descriptor{{ID: 1, Start: "", End: "", Replicas: []raft.NodeID{1}}})
	if err != nil {
		// Unreachable: a single unbounded descriptor can never fail NewMeta's overlap or
		// bounds checks. Panicking here would only ever fire if that invariant broke.
		panic(err)
	}
	return &Service{meta: meta, indices: map[uint64]Index{1: index}, vectors: map[string]vector.Vector{}}
}

// RegisterIndex attaches index under rangeID so requests meta already routes to that ID
// (via a prior or subsequent Meta.Replace) reach it -- the vector-plane counterpart of
// KVService.RegisterStore, called by cmd/consensa once a live split's migration into a
// fresh child completes.
func (s *Service) RegisterIndex(rangeID uint64, index Index) {
	s.indicesMu.Lock()
	defer s.indicesMu.Unlock()
	s.indices[rangeID] = index
}

// Meta exposes this service's routing metadata so cmd/consensa's split-execution path can
// read the current catalog (Meta.All) and publish the post-split replacement
// (Meta.Replace) without this Service needing its own bespoke split-wiring surface.
func (s *Service) Meta() *ann.Meta { return s.meta }

// indexFor resolves the index responsible for id via meta, then looks it up in indices --
// two separate locks (meta's own, and indicesMu) because they are updated by two separate
// calls (Meta.Replace, then RegisterIndex) that are not atomic with each other; a request
// arriving in the narrow window between them simply retries, the same as any ordinary
// ErrRangeKeyMismatch.
func (s *Service) indexFor(id string) (Index, error) {
	d, err := s.meta.Lookup(id)
	if err != nil {
		return nil, err
	}
	s.indicesMu.RLock()
	defer s.indicesMu.RUnlock()
	index, ok := s.indices[d.ID]
	if !ok {
		return nil, ann.ErrRangeKeyMismatch
	}
	return index, nil
}

// allIndices returns a defensive copy of every currently-registered index, for Search's
// fan-out -- a query vector carries no ID to route by, so every range that could hold a
// relevant neighbor must be searched and the results merged.
func (s *Service) allIndices() []Index {
	s.indicesMu.RLock()
	defer s.indicesMu.RUnlock()
	out := make([]Index, 0, len(s.indices))
	for _, index := range s.indices {
		out = append(out, index)
	}
	return out
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
		index, err := s.indexFor(request.Id)
		if err != nil {
			return status.Error(codes.Unavailable, err.Error())
		}
		if err := index.Validate(vector.Vector(request.Vector.Values)); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}
	for _, request := range requests {
		v := vector.Vector(request.Vector.Values)
		index, err := s.indexFor(request.Id)
		if err != nil {
			return status.Error(codes.Unavailable, err.Error())
		}
		if _, exists := s.vectors[request.Id]; exists {
			if err := index.Delete(request.Id); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
		}
		if err := index.Insert(request.Id, v); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		s.vectors[request.Id] = append(vector.Vector(nil), v...)
	}
	return stream.SendAndClose(&consensav1.UpsertResponse{Accepted: uint64(len(requests))})
}

// Search streams ordered ANN results so large result sets need not wait for response
// assembly. Unlike Upsert/Delete/BatchGet, a query vector carries no ID to route by, so
// this fans out to every currently-registered range (allIndices), merges each range's own
// top-(k+ef) candidates by ascending distance, and returns the overall top-k -- the
// standard scatter-gather shape a sharded ANN index needs once data can live in more than
// one range, matching how a real split leaves the vectors nearest any given query
// potentially on either side of the new boundary.
func (s *Service) Search(r *consensav1.SearchRequest, stream consensav1.Consensa_SearchServer) error {
	s.requestCount.Add(1)
	if r.Query == nil || len(r.Query.Values) == 0 || r.K == 0 {
		return status.Error(codes.InvalidArgument, "query and positive k are required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	indices := s.allIndices()
	var merged []ann.Result
	for _, index := range indices {
		rs, e := index.Search(vector.Vector(r.Query.Values), int(r.K), int(r.Ef))
		if e != nil {
			return status.Error(codes.InvalidArgument, e.Error())
		}
		merged = append(merged, rs...)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Distance < merged[j].Distance })
	if int(r.K) < len(merged) {
		merged = merged[:r.K]
	}
	for _, x := range merged {
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
	index, err := s.indexFor(request.Id)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if err := index.Delete(request.Id); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	delete(s.vectors, request.Id)
	return &consensav1.DeleteResponse{}, nil
}

// BatchGet returns exact vectors from the replicated index when it supports direct ID
// lookup, falling back to the local request cache for lightweight in-memory adapters.
func (s *Service) BatchGet(_ context.Context, r *consensav1.BatchGetRequest) (*consensav1.BatchGetResponse, error) {
	s.requestCount.Add(1)
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := &consensav1.BatchGetResponse{Vectors: map[string]*consensav1.Vector{}}
	for _, id := range r.Ids {
		if index, err := s.indexFor(id); err == nil {
			if getter, ok := index.(vectorGetter); ok {
				if v, ok := getter.GetVector(id); ok {
					out.Vectors[id] = &consensav1.Vector{Values: v}
					continue
				}
			}
		}
		if v, ok := s.vectors[id]; ok {
			out.Vectors[id] = &consensav1.Vector{Values: v}
		}
	}
	return out, nil
}

// Status reports this process's role for range 1 -- the original static range, always
// present -- as a representative sample; a deployment with live splits has as many roles
// as it has ranges, which this intentionally narrow single-value RPC does not attempt to
// enumerate.
func (s *Service) Status(context.Context, *consensav1.StatusRequest) (*consensav1.StatusResponse, error) {
	s.indicesMu.RLock()
	index, ok := s.indices[1]
	s.indicesMu.RUnlock()
	if !ok {
		return &consensav1.StatusResponse{Role: "single-node", Term: 0}, nil
	}
	if replicated, ok := index.(interface {
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
