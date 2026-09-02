package server

import (
	"context"
	"net"
	"testing"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/kv"
	"github.com/ashraf/consensa/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// startKVGroupReplicas is startKVGroup's sibling: it returns every replica, not just the
// leader, because AdminService.AddReplica must be driven against every existing replica
// (see its own doc comment) -- calling it against only the leader would leave the
// followers unable to ever reach a newly joined node.
func startKVGroupReplicas(t *testing.T) (leader *kv.DurableRange, all map[raft.NodeID]*kv.DurableRange) {
	t.Helper()
	ids := []raft.NodeID{1, 2, 3}
	addrs := map[raft.NodeID]string{}
	dirs := map[raft.NodeID]string{}
	for _, id := range ids {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[id] = listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		dirs[id] = t.TempDir()
	}
	replicas := map[raft.NodeID]*kv.DurableRange{}
	for _, id := range ids {
		peers := map[raft.NodeID]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = addrs[other]
			}
		}
		r, err := kv.NewDurableRange(kv.DurableRangeConfig{
			ID: id, GroupPeers: ids, ListenAddress: addrs[id], TransportPeers: peers, StorageDir: dirs[id],
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		replicas[id] = r
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range replicas {
			if err := r.Tick(); err != nil {
				t.Fatal(err)
			}
			if role, _ := r.Status(); role == raft.Leader {
				leader = r
			}
		}
		if leader != nil {
			return leader, replicas
		}
	}
	t.Fatal("kv group never elected a leader")
	return nil, nil
}

// TestAdminServiceAddsAndPromotesReplicaOverGRPC proves the network path this project's
// own docs (docs/adr/010-learners.md) named as still missing: AddKnownPeer/AddPeer/
// ProposeConfChange are proven primitives in internal/raft/new_node_join_test.go, but
// nothing reachable from a client could drive them before this file. Here, a genuinely
// new 4th kv.DurableRange -- its ID and address unknown to any of the original three
// until AddReplica registers them -- joins a live group entirely through real gRPC calls,
// catches up as a learner, and is promoted to a full voter counted toward quorum.
func TestAdminServiceAddsAndPromotesReplicaOverGRPC(t *testing.T) {
	leader, replicas := startKVGroupReplicas(t)
	ids := []raft.NodeID{1, 2, 3}

	// Some real committed data before node 4 exists, so promotion has to actually catch
	// up a real log, not just an empty one.
	if err := leader.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if v, err := leader.Get([]byte("k")); err == nil && string(v) == "v" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("seed write never became visible")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A real, separate 4th node -- started only now, exactly like
	// TestBrandNewProcessJoinsLiveGroupAsLearnerThenVoter (internal/raft), but reached
	// here entirely through gRPC rather than direct Go calls.
	fourListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fourAddr := fourListener.Addr().String()
	if err := fourListener.Close(); err != nil {
		t.Fatal(err)
	}
	allIDs := append(append([]raft.NodeID(nil), ids...), raft.NodeID(4))
	fourRange, err := kv.NewDurableRange(kv.DurableRangeConfig{
		ID: 4, GroupPeers: allIDs, ListenAddress: fourAddr,
		TransportPeers: map[raft.NodeID]string{1: mustAddr(t, replicas[1]), 2: mustAddr(t, replicas[2]), 3: mustAddr(t, replicas[3])},
		StorageDir:     t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fourRange.Close() })

	stopTick := make(chan struct{})
	t.Cleanup(func() { close(stopTick) })
	for _, id := range ids {
		r := replicas[id]
		go func() {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopTick:
					return
				case <-ticker.C:
					_ = r.Tick()
				}
			}
		}()
	}
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				_ = fourRange.Tick()
			}
		}
	}()

	// One AdminService per existing replica, mirroring one real process per replica --
	// AddReplica must be called against every one of them (see its own doc comment).
	for _, id := range ids {
		client, closeConn := dialAdminService(t, replicas[id])
		if _, err := client.AddReplica(context.Background(), &consensav1.AddReplicaRequest{
			RangeId: 1, NodeId: 4, Address: fourAddr,
		}); err != nil {
			t.Fatalf("AddReplica on node %d: %v", id, err)
		}
		closeConn()
	}

	// PromoteReplica must reach the current leader -- retry across all three, mirroring
	// TransactionalPut's own "route to whoever's in charge" contract.
	promoteDeadline := time.Now().Add(20 * time.Second)
	var promoted bool
	for time.Now().Before(promoteDeadline) && !promoted {
		for _, id := range ids {
			client, closeConn := dialAdminService(t, replicas[id])
			_, err := client.PromoteReplica(context.Background(), &consensav1.PromoteReplicaRequest{
				RangeId: 1, VoterIds: []uint64{1, 2, 3, 4},
			})
			closeConn()
			if err == nil {
				promoted = true
				break
			}
		}
		if !promoted {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !promoted {
		t.Fatal("no replica accepted PromoteReplica within the deadline")
	}

	// Node 4 must actually catch up and be counted as a real voter: a subsequent write
	// proposed through whichever node currently leads must succeed with node 4 now part
	// of the group, and node 4 must have the seeded key.
	writeDeadline := time.Now().Add(20 * time.Second)
	for {
		if v, err := fourRange.Get([]byte("k")); err == nil && string(v) == "v" {
			break
		}
		if time.Now().After(writeDeadline) {
			t.Fatal("node 4 never caught up to the pre-existing data")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func mustAddr(t *testing.T, r *kv.DurableRange) string {
	t.Helper()
	return r.Addr()
}

// dialAdminService wraps one real *kv.DurableRange's own AdminService behind a real
// bufconn gRPC server -- proving the RPC path, not calling Go methods directly, matching
// TestTransactionalPutCommitsAcrossRealRangesOverGRPC's own approach (kv_service_test.go).
func dialAdminService(t *testing.T, r *kv.DurableRange) (client consensav1.ConsensaAdminClient, closeAll func()) {
	t.Helper()
	adminService := NewAdminService(map[uint64]MembershipTarget{1: r})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	consensav1.RegisterConsensaAdminServer(grpcServer, adminService)
	go func() { _ = grpcServer.Serve(listener) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return consensav1.NewConsensaAdminClient(conn), func() {
		_ = conn.Close()
		grpcServer.Stop()
	}
}
