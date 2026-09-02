package server

import (
	"context"
	"sync"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/kv"
	"github.com/ashraf/consensa/internal/txn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVService is a separate gRPC service from Service (the vector-index plane) on purpose:
// it exposes internal/txn's cross-range 2PC coordinator, proven durable against real
// kv.DurableRange groups in internal/txn/durable_store_test.go, but until this file
// nothing reachable over the network could ever drive it -- the same
// built-but-unreachable-from-a-client gap this project found and closed for the torture
// harness's workloads and for internal/txn's own durability. This is the first thing
// that closes it: a real client-facing RPC that resolves each write's owning range
// through a real kv.Router and commits them all atomically via a real txn.Coordinator.
//
// What this deliberately does NOT do, stated plainly rather than implied by omission:
// no single-key read or write RPC (a transaction of size one is still a transaction here,
// which is correct but not the cheapest path for the common case); no automatic retry
// against kv.ErrRangeKeyMismatch (see kv.RoutedKV.retryRoute for the pattern this would
// need). cmd/consensa assembles two static DurableRange groups on its shared transport and
// registers this service beside the vector API; main_e2e_test.go proves that a real client
// reaches this path through three separate binary processes.
type KVService struct {
	consensav1.UnimplementedConsensaKVServer
	router      *kv.Router
	coordinator *txn.Coordinator
	// storesMu guards stores: it used to be fixed for the process's whole lifetime, but
	// kv.ExecuteLiveSplit now lets a background goroutine register a brand-new child
	// range's store while gRPC handler goroutines concurrently read the map in
	// TransactionalPut -- an unsynchronized map write racing a concurrent read is a real
	// data race under Go's memory model, not just theoretical, once that's possible.
	storesMu sync.RWMutex
	stores   map[uint64]txn.Participant
}

// NewKVService wires a router (resolving keys to range descriptors) and one
// txn.Participant per range ID the router can resolve to. stores is keyed by
// Descriptor.ID, matching what router.Route returns -- the caller (see
// kv_service_test.go for the pattern) builds one *txn.DurableStore per real
// kv.DurableRange group and passes them here already assembled, since only the caller
// knows how those groups were bootstrapped (addresses, storage directories, replica
// sets) and this type has no business re-deriving that.
func NewKVService(router *kv.Router, coordinator *txn.Coordinator, stores map[uint64]txn.Participant) *KVService {
	copied := make(map[uint64]txn.Participant, len(stores))
	for id, store := range stores {
		copied[id] = store
	}
	return &KVService{router: router, coordinator: coordinator, stores: copied}
}

// RegisterStore adds (or replaces) the participant for rangeID -- the piece a live split
// needs once kv.ExecuteLiveSplit finishes migrating a range's data into two fresh
// children and the caller (cmd/consensa) has called Router's underlying Meta.Replace to
// make those children routable: without this, TransactionalPut would resolve a key to a
// child range ID that Router now knows about but this service still has no participant
// for, failing every write to it with "no participant configured" forever.
func (s *KVService) RegisterStore(rangeID uint64, store txn.Participant) {
	s.storesMu.Lock()
	defer s.storesMu.Unlock()
	s.stores[rangeID] = store
}

// TransactionalPut resolves every key in the request to its owning range via router,
// groups them into one txn.WriteSet per distinct range, and commits them all atomically
// through coordinator -- a real multi-range 2PC transaction, not a loop of independent
// per-range writes that could partially apply if the process crashed midway.
func (s *KVService) TransactionalPut(_ context.Context, req *consensav1.TransactionalPutRequest) (*consensav1.TransactionalPutResponse, error) {
	if req.TxnId == "" {
		return nil, status.Error(codes.InvalidArgument, "txn_id is required")
	}
	if len(req.Writes) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one write is required")
	}

	// Group by range ID, not by store pointer identity: two keys can resolve to the same
	// range and must land in the SAME WriteSet (one Intent slice), not two separate
	// WriteSets for the same participant -- Coordinator.Prepare would install both
	// correctly either way, but a single WriteSet per range is the intended, minimal
	// shape and keeps range->intents easy to reason about, particularly on IntentKeys.
	byRange := map[uint64][]txn.Intent{}
	for key, value := range req.Writes {
		descriptor, err := s.router.Route([]byte(key))
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "routing %q: %v", key, err)
		}
		byRange[descriptor.ID] = append(byRange[descriptor.ID], txn.Intent{Key: []byte(key), Value: value})
	}

	writes := map[txn.Participant][]txn.Intent{}
	s.storesMu.RLock()
	for rangeID, intents := range byRange {
		store, ok := s.stores[rangeID]
		if !ok {
			s.storesMu.RUnlock()
			return nil, status.Errorf(codes.Internal, "no participant configured for range %d", rangeID)
		}
		writes[store] = intents
	}
	s.storesMu.RUnlock()

	if err := s.coordinator.Commit(req.TxnId, writes); err != nil {
		return nil, status.Errorf(codes.Aborted, "transaction %s: %v", req.TxnId, err)
	}
	return &consensav1.TransactionalPutResponse{}, nil
}
