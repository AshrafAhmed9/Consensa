package ann

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/vector"
)

// This file checks that a node can be stopped and restarted and get its vector index back
// from its own disk, without help from its peers.

const durableTestDim = 4

func durableTestConfig() Config {
	return Config{Dimension: durableTestDim, M: 4, EFConstruction: 20, EFSearch: 20, Seed: 7}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return addr
}

func driveDurable(nodes []*DurableNode, interval time.Duration, stop <-chan struct{}) *sync.WaitGroup {
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n *DurableNode) {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					_ = n.Tick() // errors here just mean "not leader" or a dial to a paused peer
				}
			}
		}(n)
	}
	return &wg
}

func proposeInsertToLeader(nodes []*DurableNode, id string, v vector.Vector, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	lastErr := errors.New("no replica accepted the insert")
	for time.Now().Before(end) {
		for _, n := range nodes {
			if err := n.Insert(id, v); err == nil {
				return nil
			} else {
				lastErr = err
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return lastErr
}

func waitForApplied(t *testing.T, n *DurableNode, want int, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if n.AppliedCount() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("replica applied %d mutations, want at least %d within %s", n.AppliedCount(), want, deadline)
}

// TestDurableNodeSurvivesRestart starts three replicas over real TCP, each with its own
// directory, and inserts vectors. It then stops every node, restarts node 3 alone on its
// old directory (no peers are running), and checks it can answer a nearest-neighbour
// search using only what it recovered from its own log.
func TestDurableNodeSurvivesRestart(t *testing.T) {
	ids := []raft.NodeID{1, 2, 3}
	addrs := map[raft.NodeID]string{1: freeAddr(t), 2: freeAddr(t), 3: freeAddr(t)}
	dirs := map[raft.NodeID]string{1: t.TempDir(), 2: t.TempDir(), 3: t.TempDir()}
	cfg := durableTestConfig()

	newNode := func(id raft.NodeID) *DurableNode {
		peers := map[raft.NodeID]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = addrs[other]
			}
		}
		n, err := NewDurableNode(DurableNodeConfig{
			ID: id, GroupPeers: ids, ListenAddress: addrs[id], TransportPeers: peers,
			StorageDir: dirs[id], Index: cfg,
			ElectionTick: 60, HeartbeatTick: 6,
		})
		if err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
		return n
	}

	nodes := map[raft.NodeID]*DurableNode{}
	var live []*DurableNode
	for _, id := range ids {
		n := newNode(id)
		nodes[id] = n
		live = append(live, n)
	}

	stop := make(chan struct{})
	wg := driveDurable(live, 10*time.Millisecond, stop)

	vectors := map[string]vector.Vector{
		"a": {1, 0, 0, 0},
		"b": {0, 1, 0, 0},
		"c": {0, 0, 1, 0},
	}
	inserted := 0
	for _, id := range []string{"a", "b", "c"} {
		if err := proposeInsertToLeader(live, id, vectors[id], 20*time.Second); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
		inserted++
		for _, n := range live {
			waitForApplied(t, n, inserted, 15*time.Second)
		}
	}

	// Stop all nodes. Closing node 3 closes its transport and its storage, like a crash.
	close(stop)
	wg.Wait()
	if err := nodes[3].Close(); err != nil {
		t.Fatalf("closing node 3: %v", err)
	}

	// Restart node 3 on the same directory. Peers 1 and 2 are named but not running.
	// Recovered entries are only applied when the node is driven, so one local Tick below
	// triggers the replay. A failed send to an unreachable peer must not block that
	// (see driveLocked in raft/host.go). If the search is then correct, the data came
	// entirely from node 3's own disk.
	restarted, err := NewDurableNode(DurableNodeConfig{
		ID: 3, GroupPeers: ids, ListenAddress: addrs[3],
		TransportPeers: map[raft.NodeID]string{1: addrs[1], 2: addrs[2]},
		StorageDir:     dirs[3], Index: cfg,
		ElectionTick: 60, HeartbeatTick: 6,
	})
	if err != nil {
		t.Fatalf("restarting node 3: %v", err)
	}
	defer restarted.Close()

	if err := restarted.Tick(); err != nil {
		t.Fatalf("flushing recovered log via Tick: %v", err)
	}

	if got := restarted.AppliedCount(); got != 3 {
		t.Fatalf("restarted node 3 recovered %d mutations from disk, want 3", got)
	}

	results, err := restarted.Search(vector.Vector{1, 0, 0, 0}, 1, 20)
	if err != nil {
		t.Fatalf("search after restart: %v", err)
	}
	if len(results) != 1 || results[0].ID != "a" {
		t.Fatalf("search after restart returned %+v, want nearest neighbour \"a\" recovered from disk", results)
	}

	// Clean up the two nodes that were never explicitly closed above.
	if err := nodes[1].Close(); err != nil {
		t.Errorf("closing node 1: %v", err)
	}
	if err := nodes[2].Close(); err != nil {
		t.Errorf("closing node 2: %v", err)
	}
}
