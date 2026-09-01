package kv

import (
	"errors"
	"github.com/ashraf/consensa/internal/raft"
	"time"
)

// Lease authorizes one replica to serve bounded-staleness reads until Expiration.
// It is valid only under the bounded-clock-offset assumption documented by the caller.
type Lease struct {
	Holder            raft.NodeID
	Start, Expiration time.Time
}

// ValidAt reports whether now lies in the lease interval.
func (l Lease) ValidAt(now time.Time) bool { return !now.Before(l.Start) && now.Before(l.Expiration) }

// ValidWithOffset reports whether a lease remains safe under a bounded clock-offset
// assumption. A node stops maxOffset before the nominal expiry because another healthy
// node can already be that far ahead; using the final uncertainty window would permit
// overlapping leadership beliefs after a clock skew event.
func (l Lease) ValidWithOffset(now time.Time, maxOffset time.Duration) bool {
	if maxOffset < 0 {
		return false
	}
	return !now.Before(l.Start) && now.Before(l.Expiration.Add(-maxOffset))
}

// ClosedTimestamp is a promise that no future write at or below Timestamp remains hidden.
type ClosedTimestamp struct {
	Timestamp    time.Time
	AppliedIndex uint64
}

// FollowerReadAllowed rejects reads that could miss a committed entry or exceed the closed bound.
func FollowerReadAllowed(lease Lease, holder raft.NodeID, closed ClosedTimestamp, readAt, now time.Time, applied uint64) error {
	if lease.Holder != holder || !lease.ValidAt(now) {
		return errors.New("kv: no valid follower lease")
	}
	if applied < closed.AppliedIndex {
		return errors.New("kv: replica has not applied closed timestamp")
	}
	if readAt.After(closed.Timestamp) {
		return errors.New("kv: read exceeds closed timestamp")
	}
	return nil
}

// FollowerReadAllowedWithOffset applies the conservative clock-bound lease check before
// the ordinary closed-timestamp checks. Callers that cannot state a credible offset bound
// should use ReadIndex instead of this helper.
func FollowerReadAllowedWithOffset(lease Lease, holder raft.NodeID, closed ClosedTimestamp, readAt, now time.Time, applied uint64, maxOffset time.Duration) error {
	if lease.Holder != holder || !lease.ValidWithOffset(now, maxOffset) {
		return errors.New("kv: no safe follower lease within clock offset")
	}
	if applied < closed.AppliedIndex {
		return errors.New("kv: replica has not applied closed timestamp")
	}
	if readAt.After(closed.Timestamp) {
		return errors.New("kv: read exceeds closed timestamp")
	}
	return nil
}
