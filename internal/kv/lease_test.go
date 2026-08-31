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
