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

// This file proves the actual claim in the project's README: a node can be killed and
// restarted and recovers its vector index from disk, not just from its peers. Nothing else
// in the repository tests this -- ReplicatedIndex is explicitly in-memory only (see its
// doc comment), so DurableNode (internal/ann/durable.go) and this test are what make
// "durable distributed vector store" a demonstrated fact rather than an aspiration.

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

// TestDurableNodeRejectsAllOperationsOnceRetired is DurableNode's counterpart to
// internal/kv's TestDurableRangeRejectsAllOperationsOnceRetired: it proves the zombie-parent
// bug is fixed on the vector plane too. Before MarkRetired existed, a range left running
// after a live split kept serving Insert/Delete/Search/GetVector forever for vectors the
// rest of the system now considers owned by a child range. retired is checked before
// Insert/Delete ever reach Propose, so a single never-ticked replica proves this with no
// election, group, or peer traffic needed.
func TestDurableNodeRejectsAllOperationsOnceRetired(t *testing.T) {
	n, err := NewDurableNode(DurableNodeConfig{
		ID: 1, GroupPeers: []raft.NodeID{1}, ListenAddress: freeAddr(t), TransportPeers: map[raft.NodeID]string{},
		StorageDir: t.TempDir(), Index: durableTestConfig(),
		ElectionTick: 60, HeartbeatTick: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	if n.Retired() {
		t.Fatal("freshly constructed node reports Retired() = true")
	}
	n.MarkRetired()
	if !n.Retired() {
		t.Fatal("Retired() = false after MarkRetired")
	}

	v := vector.Vector{1, 0, 0, 0}
	if err := n.Insert("a", v); !errors.Is(err, ErrRangeKeyMismatch) {
		t.Errorf("Insert on retired node = %v, want ErrRangeKeyMismatch", err)
	}
	if err := n.Delete("a"); !errors.Is(err, ErrRangeKeyMismatch) {
		t.Errorf("Delete on retired node = %v, want ErrRangeKeyMismatch", err)
	}
	if _, err := n.Search(v, 1, 10); !errors.Is(err, ErrRangeKeyMismatch) {
		t.Errorf("Search on retired node = %v, want ErrRangeKeyMismatch", err)
	}
	if got, ok := n.GetVector("a"); ok || got != nil {
		t.Errorf("GetVector on retired node = (%v, %v), want (nil, false)", got, ok)
	}
}

// TestDurableNodeFreezeRejectsSourceTraffic proves the vector merge barrier rejects an
// absorbed source before its graph can be copied into the surviving sibling group.
func TestDurableNodeFreezeRejectsSourceTraffic(t *testing.T) {
	_, live := newDurableGroupForSplit(t, 909, Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1})
	stop := make(chan struct{})
	wg := driveDurable(live, 10*time.Millisecond, stop)
	defer func() { close(stop); wg.Wait() }()
	leader := waitForAnnLeader(t, live, 10*time.Second)
	deadline := time.Now().Add(10 * time.Second)
	for !leader.Frozen() && time.Now().Before(deadline) {
		_ = leader.Freeze()
		time.Sleep(5 * time.Millisecond)
	}
	if !leader.Frozen() {
		t.Fatal("freeze barrier never applied")
	}
	if err := leader.Insert("after", vector.Vector{1, 2}); !errors.Is(err, ErrRangeKeyMismatch) {
		t.Fatalf("Insert after freeze = %v, want range mismatch", err)
	}
	if _, err := leader.Search(vector.Vector{1, 2}, 1, 4); !errors.Is(err, ErrRangeKeyMismatch) {
		t.Fatalf("Search after freeze = %v, want range mismatch", err)
	}
}

// TestDurableNodeSurvivesRestart is the headline scenario: three real Raft replicas over
// real TCP each with their own on-disk storage; insert vectors; kill one node's process
// (Close both its transport and its storage engine); restart a fresh DurableNode against
// the SAME storage directory with no peers ticking yet; and confirm it can already answer
// a correct nearest-neighbour search purely from its own recovered log -- before it has
// exchanged a single further message with the cluster.
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

	// Kill node 3 the way a real crash would: close its transport and its storage engine,
	// then stop driving it. Nodes 1 and 2 keep running underneath the closed stop channel
	// scope below -- we only stop node 3's own ticking by removing it and restarting the
	// shared driver without it.
	close(stop)
	wg.Wait()
	if err := nodes[3].Close(); err != nil {
		t.Fatalf("closing node 3: %v", err)
	}

	// Restart node 3 from the SAME storage directory, with peers 1 and 2 named but never
	// actually reachable during this test (they were stopped above and are not restarted).
	// A Host does not flush its recovered-but-unapplied entries at construction -- Ready()
	// is only inspected when something drives the node (Tick/Step/Propose), matching the
	// "no protocol decisions outside Step/Tick" contract in host.go's doc comment -- so one
	// local Tick() is required to surface the replay. That Tick is purely local computation
	// (Ready() derived from state RecoverNode already restored from disk); the internal/raft
	// host.go fix earlier in this session (a send failure must not block Apply) is what
	// makes this Tick succeed in applying the backlog even though peers 1 and 2 are
	// unreachable. If the search below is correct, recovery came entirely from disk.
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
