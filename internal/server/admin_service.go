package server

import (
	"context"
	"sync"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MembershipTarget is the subset of kv.DurableRange AdminService needs -- declared here
// rather than importing the concrete type, matching internal/txn.rangeClient's own reason
// (internal/server already imports internal/kv for KVService, but keeping this narrow
// still documents exactly what AdminService actually touches, independent of whatever
// else DurableRange grows).
type MembershipTarget interface {
	AddKnownPeer(id raft.NodeID)
	AddPeerAddress(id raft.NodeID, address string) error
	ProposeConfChange(voters, learners []raft.NodeID) error
}

// AdminService exposes internal/raft's joint-consensus membership-change primitives --
// Host.AddKnownPeer, Host.AddPeer, DurableRange.ProposeConfChange -- over gRPC, proven
// against real processes in internal/raft/new_node_join_test.go but until this file
// unreachable from a network client, the same built-but-unreachable gap this project
// found and closed for internal/txn's coordinator (KVService) and the torture harness's
// workloads before. See the ConsensaAdmin service's own doc comment (consensa.proto) for
// why this is deliberately unauthenticated, matching every other RPC in this codebase.
type AdminService struct {
	consensav1.UnimplementedConsensaAdminServer
	// ranges is keyed by the same range ID TransactionalPut's byRange grouping and
	// kv.Router.Route both use, so a caller wires this from the identical
	// map[uint64]txn.Participant-shaped assembly cmd/consensa already builds for
	// NewKVService -- one more view onto the same DurableRange instances, not a second
	// set of them.
	// rangesMu guards ranges, for the identical reason KVService.storesMu guards
	// stores: kv.ExecuteLiveSplit can register a brand-new child range from a
	// background goroutine while gRPC handler goroutines concurrently read the map.
	rangesMu sync.RWMutex
	ranges   map[uint64]MembershipTarget
}

// NewAdminService wires one MembershipTarget per range ID this process hosts. Unlike
// NewKVService, which needs its stores to satisfy txn.Participant, this needs the
// narrower MembershipTarget -- any *kv.DurableRange satisfies both, so a caller typically
// passes the identical range instances to both constructors.
func NewAdminService(ranges map[uint64]MembershipTarget) *AdminService {
	copied := make(map[uint64]MembershipTarget, len(ranges))
	for id, target := range ranges {
		copied[id] = target
	}
	return &AdminService{ranges: copied}
}

// RegisterRange adds (or replaces) the membership target for rangeID -- the AdminService
// counterpart to KVService.RegisterStore, called for the same reason: once a live split
// creates a new child range, this service needs to know about it too, or a subsequent
// AddReplica/PromoteReplica naming that range ID would fail with NotFound forever.
func (s *AdminService) RegisterRange(rangeID uint64, target MembershipTarget) {
	s.rangesMu.Lock()
	defer s.rangesMu.Unlock()
	s.ranges[rangeID] = target
}

// AddReplica registers a new node's ID and address on the named range's local Raft state
// -- see ConsensaAdmin.AddReplica's own doc comment (consensa.proto) for why this and
// PromoteReplica are separate calls. Unlike PromoteReplica, this succeeds on ANY replica
// of the range, not just the leader: AddKnownPeer/AddPeerAddress are local, per-replica
// bookkeeping that never touches Raft's own commit protocol (see their own doc comments
// in internal/raft), so a caller must invoke this once per existing replica -- calling it
// against only the leader would leave the followers unable to ever reach the new node.
func (s *AdminService) AddReplica(_ context.Context, req *consensav1.AddReplicaRequest) (*consensav1.AddReplicaResponse, error) {
	s.rangesMu.RLock()
	target, ok := s.ranges[req.RangeId]
	s.rangesMu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no range %d on this node", req.RangeId)
	}
	if req.NodeId == 0 {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if req.Address == "" {
		return nil, status.Error(codes.InvalidArgument, "address is required")
	}
	id := raft.NodeID(req.NodeId)
	target.AddKnownPeer(id)
	if err := target.AddPeerAddress(id, req.Address); err != nil {
		return nil, status.Errorf(codes.Internal, "registering peer address: %v", err)
	}
	return &consensav1.AddReplicaResponse{}, nil
}

// PromoteReplica drives ProposeConfChange against the named range, which only succeeds
// against the range's current leader -- the caller is expected to retry against a
// different node on a FailedPrecondition, the same contract TransactionalPut's routing
// failures already establish for this codebase's other admin-adjacent RPC.
func (s *AdminService) PromoteReplica(_ context.Context, req *consensav1.PromoteReplicaRequest) (*consensav1.PromoteReplicaResponse, error) {
	s.rangesMu.RLock()
	target, ok := s.ranges[req.RangeId]
	s.rangesMu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no range %d on this node", req.RangeId)
	}
	if len(req.VoterIds) == 0 {
		return nil, status.Error(codes.InvalidArgument, "voter_ids must be non-empty")
	}
	voters := make([]raft.NodeID, len(req.VoterIds))
	for i, id := range req.VoterIds {
		voters[i] = raft.NodeID(id)
	}
	learners := make([]raft.NodeID, len(req.LearnerIds))
	for i, id := range req.LearnerIds {
		learners[i] = raft.NodeID(id)
	}
	if err := target.ProposeConfChange(voters, learners); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "proposing conf change on range %d: %v", req.RangeId, err)
	}
	return &consensav1.PromoteReplicaResponse{}, nil
}
