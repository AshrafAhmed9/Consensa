package raft

import (
	"net"
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/storage"
)

// TestHostsElectAndReplicateOverTCP proves the impure runtime preserves the pure-node
// ordering while two separate durable stores exchange real TCP Raft messages.
func TestHostsElectAndReplicateOverTCP(t *testing.T) {
	address1, address2 := reserveAddress(t), reserveAddress(t)
	db1, err := storage.Open(storage.Options{Dir: t.TempDir(), SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	db2, err := storage.Open(storage.Options{Dir: t.TempDir(), SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	applied1, applied2 := make(chan string, 1), make(chan string, 1)
	peers := map[NodeID]string{1: address1, 2: address2}
	host1, err := NewHost(HostConfig{Raft: Config{ID: 1, Peers: []NodeID{1, 2}, ElectionTick: 3, HeartbeatTick: 1}, ListenAddress: address1, Peers: peers, Persister: NewPersister(db1), Apply: func(entry Entry) error { applied1 <- string(entry.Data); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer host1.Close()
	host2, err := NewHost(HostConfig{Raft: Config{ID: 2, Peers: []NodeID{1, 2}, ElectionTick: 5, HeartbeatTick: 1}, ListenAddress: address2, Peers: peers, Persister: NewPersister(db2), Apply: func(entry Entry) error { applied2 <- string(entry.Data); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer host2.Close()
	// Node 1 deliberately has the shorter timeout. Drive it to election while node 2
	// remains event-driven; ticking both as fast as a unit test can run is not a model of
	// wall-clock timers and can manufacture repeated elections before TCP handlers run.
	for i := 0; i < 4; i++ {
		if err := host1.Tick(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	deadline := time.Now().Add(time.Second)
	for {
		err = host1.Propose([]byte("set value"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no leader elected: %v", err)
		}
		if err := host1.Tick(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The leader learns commitment from the append response. Followers learn its new
	// commit index in a subsequent heartbeat, exactly as Raft's AppendEntries protocol
	// specifies. TCP delivery is asynchronous, so keep driving until both applications
	// arrive or the bounded test deadline expires.
	for i := 0; i < 20; i++ {
		if err := host1.Tick(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, applied := range []chan string{applied1, applied2} {
		select {
		case got := <-applied:
			if got != "set value" {
				t.Fatalf("applied %q", got)
			}
		case <-time.After(time.Second):
			host1.mu.Lock()
			node1 := host1.node.(*node)
			host2.mu.Lock()
			node2 := host2.node.(*node)
			t.Fatalf("timed out waiting for replicated apply: node1 role=%v term=%d commit=%d applied=%d last=%d match=%v next=%v; node2 role=%v term=%d commit=%d applied=%d last=%d", node1.role, node1.term, node1.log.committed, node1.log.applied, node1.log.lastIndex(), node1.match, node1.next, node2.role, node2.term, node2.log.committed, node2.log.applied, node2.log.lastIndex())
		}
	}
}

func reserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
