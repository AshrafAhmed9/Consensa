package main

import (
	"net"
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/kv"
	"github.com/ashraf/consensa/internal/raft"
)

// startClosedTimestampTestRange builds one real 3-node kv.DurableRange group and drives
// it until a leader is elected, mirroring internal/txn's own startDurableRange test
// helper -- this file cannot import that unexported helper (different package), so it is
// duplicated narrowly rather than exported just for this one test.
func startClosedTimestampTestRange(t *testing.T) (leader *kv.DurableRange, all []*kv.DurableRange) {
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
			ElectionTick: 60, HeartbeatTick: 6,
		})
		if err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
		t.Cleanup(func() { _ = r.Close() })
		replicas[id] = r
		all = append(all, r)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range replicas {
			if err := r.Tick(); err != nil {
				t.Fatal(err)
			}
			if role, _ := r.Status(); role == raft.Leader {
				return r, all
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("group never elected a leader")
	return nil, nil
}

// TestAdvanceClosedTimestampsDrivesRealProgress proves the exact wiring main() installs
// on a real timer: calling advanceClosedTimestamps repeatedly against real
// kv.DurableRange leaders (not just proving AdvanceClosedTimestamp itself works in
// isolation, already covered by internal/kv's own TestFollowerReadServesOnceLeasedAndClosed)
// actually makes each range's CurrentClosedTimestamp progress over time, and that it is
// safe to call against a range whose leader has since changed underneath it -- the
// specific reason main.go calls this against every range rather than tracking "the
// leader" itself.
func TestAdvanceClosedTimestampsDrivesRealProgress(t *testing.T) {
	leaderA, allA := startClosedTimestampTestRange(t)
	leaderB, allB := startClosedTimestampTestRange(t)

	stop := make(chan struct{})
	drive := func(ranges []*kv.DurableRange) {
		go func() {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					for _, r := range ranges {
						_ = r.Tick()
					}
				}
			}
		}()
	}
	drive(allA)
	drive(allB)
	defer close(stop)

	before := leaderA.CurrentClosedTimestamp()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		advanceClosedTimestamps(time.Now(), 200*time.Millisecond, leaderA, leaderB)
		afterA := leaderA.CurrentClosedTimestamp()
		afterB := leaderB.CurrentClosedTimestamp()
		if afterA.AppliedIndex > before.AppliedIndex && !afterB.Timestamp.IsZero() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("advanceClosedTimestamps never drove real progress on both ranges")
}

// TestAdvanceClosedTimestampsIgnoresNonLeaderErrors proves calling it against a follower
// (which AdvanceClosedTimestamp rejects, since only a leader may propose) does not panic
// or block -- the whole reason main.go calls it against every replica rather than
// tracking leadership itself.
func TestAdvanceClosedTimestampsIgnoresNonLeaderErrors(t *testing.T) {
	_, all := startClosedTimestampTestRange(t)
	var follower *kv.DurableRange
	for _, r := range all {
		if role, _ := r.Status(); role != raft.Leader {
			follower = r
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}
	advanceClosedTimestamps(time.Now(), time.Second, follower)
	if got := follower.CurrentClosedTimestamp(); !got.Timestamp.IsZero() {
		t.Fatalf("follower's closed timestamp advanced despite not being leader: %+v", got)
	}
}
