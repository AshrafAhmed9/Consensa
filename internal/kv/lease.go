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
