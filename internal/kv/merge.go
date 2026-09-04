package kv

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/ashraf/consensa/internal/raft"
)

// MergeTrigger requires both neighbouring ranges to be below their size and QPS floors.
// Collapsing a cold range into a hot neighbour would merely recreate the hotspot a split
// isolated, so a one-sided decision cannot satisfy Phase 12's load-shedding invariant.
type MergeTrigger struct {
	SizeFloor                   int
	LeftQPS, RightQPS, QPSFloor float64
}

// ShouldMerge decides whether two adjacent key ranges are cold enough to consolidate.
func ShouldMerge(trigger MergeTrigger, left, right map[string][]byte) bool {
	return trigger.SizeFloor > 0 && len(left)+len(right) <= trigger.SizeFloor &&
		trigger.QPSFloor > 0 && trigger.LeftQPS <= trigger.QPSFloor && trigger.RightQPS <= trigger.QPSFloor
}

// MergeDescriptors combines adjacent ranges with identical replica placement. The metadata
// transaction that publishes the result must first establish the coordinated Raft barrier;
// this function only protects the range-ownership invariant at that boundary.
func MergeDescriptors(left, right Descriptor, mergedID uint64) (Descriptor, error) {
	if mergedID == 0 || !bytes.Equal(left.End, right.Start) {
		return Descriptor{}, errors.New("kv: ranges are not adjacent")
	}
	if len(left.Replicas) != len(right.Replicas) {
		return Descriptor{}, errors.New("kv: replica sets differ")
	}
	for i, replica := range left.Replicas {
		if replica != right.Replicas[i] {
			return Descriptor{}, errors.New("kv: replica sets differ")
		}
	}
	return Descriptor{ID: mergedID, Start: append([]byte(nil), left.Start...), End: append([]byte(nil), right.End...), Replicas: append([]raft.NodeID(nil), left.Replicas...)}, nil
}

// ExecuteLiveMerge copies the frozen right range into the already-running left group
// before its descriptor grows. Reusing left prevents a retired split parent from ever
// becoming authoritative again with an obsolete Raft history.
func ExecuteLiveMerge(left, right Descriptor, rightData map[string][]byte, target splitTarget, timeout time.Duration) (Descriptor, error) {
	merged, err := MergeDescriptors(left, right, left.ID)
	if err != nil {
		return Descriptor{}, err
	}
	for key, value := range rightData {
		if !right.Contains([]byte(key)) {
			return Descriptor{}, fmt.Errorf("kv: merge source key %q is outside the absorbed descriptor", key)
		}
		if err := putAndConfirm(target, []byte(key), value, timeout); err != nil {
			return Descriptor{}, fmt.Errorf("kv: merge %q: %w", key, err)
		}
	}
	return merged, nil
}
