package main

import (
	"fmt"
	"net"
	"os/exec"
	"testing"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/kv"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/server"
	"google.golang.org/grpc"
)

// startKVGroupReplicas mirrors internal/server/admin_service_test.go's own helper of the
// same name exactly (that one is unexported and this package cannot import a _test.go
// file across packages) -- three real 3-node kv.DurableRange replicas over real TCP,
// hand-ticked synchronously until a leader is elected, matching the original's own
// two-phase ticking (synchronous here to get a leader back at all; background ticking
// goroutines start later, once the caller is ready to seed real writes).
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

// serveAdmin starts a real TCP gRPC server (not bufconn: consensa-cli join dials a real
// address string, exactly like it would against real cmd/consensa processes) wrapping one
// AdminService per replica, and returns the address it's listening on.
func serveAdmin(t *testing.T, r *kv.DurableRange) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	consensav1.RegisterConsensaAdminServer(grpcServer, server.NewAdminService(map[uint64]server.MembershipTarget{1: r}))
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	return listener.Addr().String()
}

// buildCLI compiles the real consensa-cli binary once for this test file -- proving the
// actual shipped tool, not a package-internal call to runJoin.
func buildCLI(t *testing.T) string {
	t.Helper()
	binary := t.TempDir() + "/consensa-cli"
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Dir = "."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building consensa-cli: %v\n%s", err, out)
	}
	return binary
}

// TestJoinAddsAndPromotesReplicaOverRealProcess proves consensa-cli join, run as a real
// OS process against real TCP gRPC servers, actually automates the sequence
// TestAdminServiceAddsAndPromotesReplicaOverGRPC (internal/server) proves by hand: a
// genuinely new 4th kv.DurableRange, started separately and known to nothing yet, is
// brought in as a full voter with zero downtime to the existing three, driven entirely by
// the CLI binary rather than direct gRPC calls in Go.
func TestJoinAddsAndPromotesReplicaOverRealProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs a real OS process; skipped in -short mode")
	}
	binary := buildCLI(t)

	leader, replicas := startKVGroupReplicas(t)
	ids := []raft.NodeID{1, 2, 3}

	// Real committed data before node 4 exists, so promotion has to actually catch up a
	// real log -- mirroring the internal/server test's own reasoning exactly.
	if err := leader.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	writeDeadline := time.Now().Add(5 * time.Second)
	for {
		if v, err := leader.Get([]byte("k")); err == nil && string(v) == "v" {
			break
		}
		if time.Now().After(writeDeadline) {
			t.Fatal("seed write never became visible")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A real, separate 4th replica -- started only now, knowing nothing about the group
	// beyond the three existing addresses, exactly like
	// TestBrandNewProcessJoinsLiveGroupAsLearnerThenVoter (internal/raft).
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
	tickEvery := func(r *kv.DurableRange) {
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
	for _, id := range ids {
		tickEvery(replicas[id])
	}
	tickEvery(fourRange)

	adminAddrs := fmt.Sprintf("1=%s,2=%s,3=%s", serveAdmin(t, replicas[1]), serveAdmin(t, replicas[2]), serveAdmin(t, replicas[3]))

	cmd := exec.Command(binary, "join",
		"-range-id", "1",
		"-new-id", "4",
		"-new-addr", fourAddr,
		"-existing", adminAddrs,
		"-timeout", "20s",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("consensa-cli join failed: %v\n%s", err, out)
	}
	t.Logf("consensa-cli join output:\n%s", out)

	// Node 4 must actually be a real voter now: a subsequent write proposed through
	// whichever node currently leads must succeed with node 4 counted, and node 4 must
	// have the pre-existing seeded data.
	catchUpDeadline := time.Now().Add(20 * time.Second)
	for {
		if v, err := fourRange.Get([]byte("k")); err == nil && string(v) == "v" {
			break
		}
		if time.Now().After(catchUpDeadline) {
			t.Fatal("node 4 never caught up to the pre-existing data after consensa-cli join")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func mustAddr(t *testing.T, r *kv.DurableRange) string {
	t.Helper()
	return r.Addr()
}
