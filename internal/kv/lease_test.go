package kv

import (
	"github.com/ashraf/consensa/internal/raft"
	"testing"
	"time"
)

// TestFollowerReadRequiresClosedAppliedState proves a lease alone cannot make stale state safe.
func TestFollowerReadRequiresClosedAppliedState(t *testing.T) {
	now := time.Unix(10, 0)
	lease := Lease{Holder: raft.NodeID(2), Start: now.Add(-time.Second), Expiration: now.Add(time.Second)}
	closed := ClosedTimestamp{Timestamp: now, AppliedIndex: 5}
	if e := FollowerReadAllowed(lease, 2, closed, now, now, 4); e == nil {
		t.Fatal("stale follower read allowed")
	}
	if e := FollowerReadAllowed(lease, 2, closed, now, now, 5); e != nil {
		t.Fatal(e)
	}
}

// TestFollowerReadRejectsLeaseInsideClockUncertainty proves the explicit max-offset
// margin prevents a locally valid-looking lease from being used at its unsafe tail.
func TestFollowerReadRejectsLeaseInsideClockUncertainty(t *testing.T) {
	now := time.Unix(10, 0)
	lease := Lease{Holder: raft.NodeID(2), Start: now.Add(-time.Second), Expiration: now.Add(50 * time.Millisecond)}
	closed := ClosedTimestamp{Timestamp: now, AppliedIndex: 5}
	if err := FollowerReadAllowed(lease, 2, closed, now, now, 5); err != nil {
		t.Fatalf("ordinary lease check unexpectedly failed: %v", err)
	}
	if err := FollowerReadAllowedWithOffset(lease, 2, closed, now, now, 5, 100*time.Millisecond); err == nil {
		t.Fatal("lease inside clock uncertainty window was accepted")
	}
}
