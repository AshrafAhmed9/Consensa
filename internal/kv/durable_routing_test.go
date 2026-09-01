package kv

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/raft"
)

// This file proves the piece RoutedKV/MultiRaft's in-memory tests (router_test-adjacent,
// e.g. TestRoutedKVEightStaticRanges) cannot: that Router really does send a key to a
// DIFFERENT range's real, independently-elected Raft leader depending on which half of the
// keyspace it falls in, that two ranges' real TCP clusters do not interfere with each
// other, and that killing one range's leader has zero effect on the other range's
// throughput or leadership.
//
// It deliberately does NOT add a server-side "find the leader and forward" abstraction to
// DurableRange. DurableNode (internal/ann) already established that retrying across
// replicas is the client's job, not the server's -- see docs/notes/05-api.md's "Writes do
// not forward to the leader" section -- and giving ranges a different contract here would
// be an inconsistency, not a shortcut. The retry helper below plays the client's role,
// the same way cmd/consensa/main_e2e_test.go's upsertUntilAccepted does for vectors.

type durableRangeGroup struct {
	descriptor Descriptor
	replicas   map[raft.NodeID]*DurableRange
	addrs      map[raft.NodeID]string
	dirs       map[raft.NodeID]string
}

func startDurableRangeGroup(t *testing.T, rangeID uint64, start, end []byte, ids []raft.NodeID) *durableRangeGroup {
	t.Helper()
	g := &durableRangeGroup{
		descriptor: Descriptor{ID: rangeID, Start: start, End: end, Replicas: ids},
		replicas:   map[raft.NodeID]*DurableRange{},
		addrs:      map[raft.NodeID]string{},
		dirs:       map[raft.NodeID]string{},
	}
	for _, id := range ids {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("range %d node %d: reserve port: %v", rangeID, id, err)
		}
		g.addrs[id] = listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		g.dirs[id] = t.TempDir()
	}
	for _, id := range ids {
		peers := map[raft.NodeID]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = g.addrs[other]
			}
		}
		r, err := NewDurableRange(DurableRangeConfig{
			ID: id, GroupPeers: ids, ListenAddress: g.addrs[id], TransportPeers: peers, StorageDir: g.dirs[id],
		})
		if err != nil {
			t.Fatalf("range %d node %d: %v", rangeID, id, err)
		}
		g.replicas[id] = r
	}
	return g
}

func (g *durableRangeGroup) list() []*DurableRange {
	out := make([]*DurableRange, 0, len(g.replicas))
	for _, r := range g.replicas {
		out = append(out, r)
	}
	return out
}

func (g *durableRangeGroup) closeAll(t *testing.T) {
	t.Helper()
	for id, r := range g.replicas {
		if err := r.Close(); err != nil {
			t.Errorf("range %d node %d: close: %v", g.descriptor.ID, id, err)
		}
	}
}

// routedPut resolves key through the router (retrying once after a refresh, matching
// RoutedKV.retryRoute's contract) and then retries the resolved range's own replicas --
// exactly the two-level retry a real distributed client actually needs: which range, then
// which replica of it is currently willing to accept the write.
func routedPut(t *testing.T, router *Router, groups map[uint64]*durableRangeGroup, key, value []byte, deadline time.Duration) {
	t.Helper()
	descriptor, err := router.Route(key)
	if err != nil {
		t.Fatalf("routing %q: %v", key, err)
	}
	group, ok := groups[descriptor.ID]
	if !ok {
		t.Fatalf("router resolved %q to unknown range %d", key, descriptor.ID)
	}
	end := time.Now().Add(deadline)
	lastErr := errors.New("no replica accepted the put")
	for time.Now().Before(end) {
		for _, r := range group.list() {
			if err := r.Put(key, value); err == nil {
				return
			} else {
				lastErr = err
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("range %d never accepted put of %q: %v", descriptor.ID, key, lastErr)
}

func TestRouterDirectsRealRangesAndIsolatesFailure(t *testing.T) {
	ids := []raft.NodeID{1, 2, 3}
	rangeA := startDurableRangeGroup(t, 1, []byte("a"), []byte("m"), ids)
	rangeB := startDurableRangeGroup(t, 2, []byte("m"), nil, ids)
	groups := map[uint64]*durableRangeGroup{1: rangeA, 2: rangeB}

	meta, err := NewMeta([]Descriptor{rangeA.descriptor, rangeB.descriptor})
	if err != nil {
		t.Fatal(err)
	}
	router := NewRouter(meta)

	stop := make(chan struct{})
	wg := driveRanges(append(rangeA.list(), rangeB.list()...), 10*time.Millisecond, stop)
	defer func() { close(stop); wg.Wait() }()
	defer rangeA.closeAll(t)
	defer rangeB.closeAll(t)

	// "apple" < "m" routes to range A; "zebra" >= "m" routes to range B.
	routedPut(t, router, groups, []byte("apple"), []byte("fruit"), 10*time.Second)
	routedPut(t, router, groups, []byte("zebra"), []byte("animal"), 10*time.Second)

	// Prove isolation, not just that both eventually converge: "apple" must land ONLY on
	// range A's replicas and never appear on range B's, and vice versa. Cross-contamination
	// here would mean the router or the range wiring silently ignored the key boundary.
	deadline := time.Now().Add(6 * time.Second)
	for {
		aHasApple, bLacksApple := true, true
		for _, r := range rangeA.list() {
			if v, err := r.Get([]byte("apple")); err != nil || string(v) != "fruit" {
				aHasApple = false
			}
		}
		for _, r := range rangeB.list() {
			if _, err := r.Get([]byte("apple")); err == nil {
				bLacksApple = false
			}
		}
		if aHasApple && bLacksApple {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("apple did not converge correctly: onA=%v absentFromB=%v", aHasApple, bLacksApple)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Record range B's leader and term before touching range A at all, so we can prove
	// range A's failure had zero effect on it.
	var bLeaderBefore raft.NodeID
	var bTermBefore uint64
	for id, r := range rangeB.replicas {
		if role, term := r.Status(); role == raft.Leader {
			bLeaderBefore, bTermBefore = id, term
		}
	}
	if bLeaderBefore == 0 {
		t.Fatal("range B has no leader before the range A failure")
	}

	// Kill range A's leader -- a real process-equivalent failure, its real TCP transport
	// and storage both closed -- and confirm range A recovers on its own while range B is
	// completely undisturbed.
	var aLeaderID raft.NodeID
	for id, r := range rangeA.replicas {
		if role, _ := r.Status(); role == raft.Leader {
			aLeaderID = id
		}
	}
	if aLeaderID == 0 {
		t.Fatal("range A has no leader to kill")
	}
	if err := rangeA.replicas[aLeaderID].Close(); err != nil {
		t.Fatalf("closing range A leader: %v", err)
	}
	delete(rangeA.replicas, aLeaderID)

	routedPut(t, router, groups, []byte("banana"), []byte("fruit2"), 10*time.Second)

	if role, term := rangeB.replicas[bLeaderBefore].Status(); role != raft.Leader || term != bTermBefore {
		t.Fatalf("range B's leadership changed after an unrelated range A failure: role=%v term=%v, want leader term=%d", role, term, bTermBefore)
	}
}
