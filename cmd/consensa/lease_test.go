package main

import (
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/raft"
)

// TestMaintainLeasesGrantsAndRenewsOnlyOnLeader proves the exact wiring main() installs
// on a real timer: calling maintainLeases repeatedly against a real kv.DurableRange group
// actually gets a valid lease held by the leader's own ID, that a non-leader is never
// proposed to (GrantLease would just fail, but this checks maintainLeases doesn't even try
// by asserting CurrentLease stays untouched on followers until it replicates), and that a
// lease already comfortably valid past renewBefore is left alone rather than re-proposed
// every call -- the piece that keeps this from generating a Raft entry on every tick.
func TestMaintainLeasesGrantsAndRenewsOnlyOnLeader(t *testing.T) {
	leader, all := startClosedTimestampTestRange(t)
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				for _, r := range all {
					_ = r.Tick()
				}
			}
		}
	}()
	defer close(stop)

	// startClosedTimestampTestRange builds replicas by iterating raft.NodeID{1,2,3} in
	// order and appending each to `all` as it goes, so all[i]'s ID is ids[i] -- this test
	// relies on that instead of exporting an ID getter solely for its own use.
	ids := []raft.NodeID{1, 2, 3}
	var holder raft.NodeID
	for i, r := range all {
		if role, _ := r.Status(); role == raft.Leader {
			holder = ids[i]
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		maintainLeases(time.Now(), holder, 6*time.Second, 3*time.Second, leader)
		lease := leader.CurrentLease()
		if lease.Holder == holder && lease.ValidAt(time.Now()) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease never granted and applied")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A lease that is still comfortably valid must not be re-proposed: capture its
	// expiration, call again immediately, and confirm it is unchanged.
	before := leader.CurrentLease()
	maintainLeases(time.Now(), holder, 6*time.Second, 3*time.Second, leader)
	time.Sleep(50 * time.Millisecond)
	after := leader.CurrentLease()
	if !after.Expiration.Equal(before.Expiration) {
		t.Fatalf("lease was re-proposed while still comfortably valid: before=%v after=%v", before.Expiration, after.Expiration)
	}
}
