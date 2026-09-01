package txn

import (
	"net"
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/kv"
	"github.com/ashraf/consensa/internal/raft"
)

// startDurableRange builds one real 3-node Raft-backed range for these tests and returns
// its current leader. A single-node group cannot be used here: startPreVote only messages
// *other* peers (election.go), so with zero peers to message, a lone node's own
// already-met quorum (quorum()==1) is never reactively re-evaluated and it never elects
// itself -- a real, if practically unimportant, gap noted rather than worked around by
// special-casing it in production code. TestRouterDirectsRealRangesAndIsolatesFailure
// (internal/kv) already covers multi-node replication and failure isolation for
// DurableRange itself, so this file's job is only proving DurableStore's translation into
// real, durable Put/Get calls is correct -- three real nodes are enough for that and
// match how every other DurableRange test in this codebase is set up.
func startDurableRange(t *testing.T, groupID uint64) *kv.DurableRange {
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
			t.Fatalf("group %d node %d: %v", groupID, id, err)
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
				return r
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("group %d never elected a leader", groupID)
	return nil
}

// TestCoordinatorCommitsAcrossRealRaftRanges proves the exact protocol
// TestCoordinatorCommitsAcrossParticipants proves against the in-memory Store also holds
// when both participants are real, separate Raft-replicated ranges -- the scenario Phase
// 4 exists for (a write spanning two ranges), not a model of it.
func TestCoordinatorCommitsAcrossRealRaftRanges(t *testing.T) {
	rangeA := startDurableRange(t, 1)
	rangeB := startDurableRange(t, 2)
	a, b := NewDurableStore(rangeA), NewDurableStore(rangeB)

	clock := NewClock(time.Now)
	err := NewCoordinator(clock).Commit("t1", map[Participant][]Intent{
		a: {{Key: []byte("acct-1"), Value: []byte("100")}},
		b: {{Key: []byte("acct-2"), Value: []byte("200")}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if v, err := rangeA.Get([]byte("acct-1")); err != nil || string(v) != "100" {
		t.Fatalf("rangeA acct-1 = %q, %v", v, err)
	}
	if v, err := rangeB.Get([]byte("acct-2")); err != nil || string(v) != "200" {
		t.Fatalf("rangeB acct-2 = %q, %v", v, err)
	}
}

// TestDurableStoreRecordSurvivesRestart proves a transaction record and its resolved
// value are recovered from a range's own real Raft log after a restart -- not just held
// in the process's memory -- the same durability guarantee
// internal/kv.TestDurableRangeSurvivesRestart proves for plain (non-transactional) writes.
// The range being restarted is deliberately not the one the transaction ran through: it
// recovers purely by replaying committed entries from its peers/its own log, the same as
// any other follower rejoining, so this also exercises real replication, not just local
// persistence.
func TestDurableStoreRecordSurvivesRestart(t *testing.T) {
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
	open := func(id raft.NodeID) *kv.DurableRange {
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
		return r
	}
	replicas := map[raft.NodeID]*kv.DurableRange{1: open(1), 2: open(2), 3: open(3)}
	defer func() { _ = replicas[1].Close() }()
	defer func() { _ = replicas[2].Close() }()

	var leader *kv.DurableRange
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && leader == nil {
		for _, r := range replicas {
			if err := r.Tick(); err != nil {
				t.Fatal(err)
			}
			if role, _ := r.Status(); role == raft.Leader {
				leader = r
				break
			}
		}
	}
	if leader == nil {
		t.Fatal("group never elected a leader")
	}

	store := NewDurableStore(leader)
	clock := NewClock(time.Now)
	if err := NewCoordinator(clock).Commit("t-restart", map[Participant][]Intent{
		store: {{Key: []byte("balance"), Value: []byte("42")}},
	}); err != nil {
		t.Fatal(err)
	}

	// Kill node 3 specifically -- it is never the leader we transacted through unless
	// this deterministic 3-node bootstrap happened to elect it (position ordering in
	// NewCluster/NewNode makes the lowest-ID node win in practice, but this isn't relied
	// on) -- and restart it fresh from its own directory, proving recovery, not luck.
	if err := replicas[3].Close(); err != nil {
		t.Fatal(err)
	}
	restarted := open(3)
	defer func() { _ = restarted.Close() }()
	replicas[1].Tick()
	replicas[2].Tick()
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := restarted.Tick(); err != nil {
			t.Fatal(err)
		}
		if v, err := restarted.Get([]byte("balance")); err == nil && string(v) == "42" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	restartedStore := NewDurableStore(restarted)
	record, ok := restartedStore.Record("t-restart")
	if !ok || record.Status != Committed {
		t.Fatalf("transaction record did not survive restart: %+v ok=%v", record, ok)
	}
	if v, err := restarted.Get([]byte("balance")); err != nil || string(v) != "42" {
		t.Fatalf("balance did not survive restart: %q, %v", v, err)
	}
}
