package server

import (
	"context"
	"net"
	"testing"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/kv"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/txn"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// startKVGroup builds one real 3-node kv.DurableRange group and waits for it to elect a
// leader -- the same pattern internal/txn/durable_store_test.go uses, duplicated here
// rather than exported across packages since it is test-only setup, not production code.
func startKVGroup(t *testing.T) *kv.DurableRange {
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
	var leader *kv.DurableRange
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && leader == nil {
		for _, r := range replicas {
			if err := r.Tick(); err != nil {
				t.Fatal(err)
			}
			if role, _ := r.Status(); role == raft.Leader {
				leader = r
			}
		}
	}
	if leader == nil {
		t.Fatal("kv group never elected a leader")
	}
	return leader
}

// TestTransactionalPutCommitsAcrossRealRangesOverGRPC proves the whole path a client
// actually uses end to end: a real gRPC call, resolved by a real kv.Router to two real,
// independent 3-node kv.DurableRange groups, committed atomically by a real
// txn.Coordinator. internal/txn/durable_store_test.go already proves the coordinator
// itself works against real ranges when driven directly from Go; this test proves the
// same property is reachable from a network client, which is what actually matters for
// the "linearizable KV plane" claim in PLAN.md's Claims Discipline section to mean
// anything to an external caller.
func TestTransactionalPutCommitsAcrossRealRangesOverGRPC(t *testing.T) {
	rangeA := startKVGroup(t)
	rangeB := startKVGroup(t)

	meta, err := kv.NewMeta([]kv.Descriptor{
		{ID: 1, Start: []byte("a"), End: []byte("m"), Replicas: []raft.NodeID{1}},
		{ID: 2, Start: []byte("m"), End: nil, Replicas: []raft.NodeID{1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	router := kv.NewRouter(meta)
	coordinator := txn.NewCoordinator(txn.NewClock(time.Now))
	stores := map[uint64]txn.Participant{
		1: txn.NewDurableStore(rangeA),
		2: txn.NewDurableStore(rangeB),
	}
	kvService := NewKVService(router, coordinator, stores)

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	consensav1.RegisterConsensaKVServer(grpcServer, kvService)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := consensav1.NewConsensaKVClient(conn)

	// "apple" routes to range A ([a, m)), "melon" routes to range B ([m, +inf)) -- one
	// real transaction spanning both real Raft groups.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.TransactionalPut(ctx, &consensav1.TransactionalPutRequest{
		TxnId: "t-1",
		Writes: map[string][]byte{
			"apple": []byte("fruit-a"),
			"melon": []byte("fruit-b"),
		},
	})
	if err != nil {
		t.Fatalf("TransactionalPut: %v", err)
	}

	if v, err := rangeA.Get([]byte("apple")); err != nil || string(v) != "fruit-a" {
		t.Fatalf("rangeA apple = %q, %v", v, err)
	}
	if v, err := rangeB.Get([]byte("melon")); err != nil || string(v) != "fruit-b" {
		t.Fatalf("rangeB melon = %q, %v", v, err)
	}
}

// TestTransactionalPutRejectsUnroutableKey proves a key outside every known range's span
// fails the whole request rather than silently dropping that key's write.
func TestTransactionalPutRejectsUnroutableKey(t *testing.T) {
	meta, err := kv.NewMeta([]kv.Descriptor{
		{ID: 1, Start: []byte("a"), End: []byte("m"), Replicas: []raft.NodeID{1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	router := kv.NewRouter(meta)
	coordinator := txn.NewCoordinator(txn.NewClock(time.Now))
	kvService := NewKVService(router, coordinator, map[uint64]txn.Participant{})

	_, err = kvService.TransactionalPut(context.Background(), &consensav1.TransactionalPutRequest{
		TxnId:  "t-2",
		Writes: map[string][]byte{"zebra": []byte("out-of-range")},
	})
	if err == nil {
		t.Fatal("expected an error for a key outside every known range")
	}
}
