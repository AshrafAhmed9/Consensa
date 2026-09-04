package ann

import (
	"errors"
	"fmt"
	"time"

	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/vector"
)

// MergeTrigger requires both adjacent ranges to be cold. This prevents an automatic
// merge from immediately undoing the load isolation that caused the split.
type MergeTrigger struct {
	SizeFloor                   int
	LeftQPS, RightQPS, QPSFloor float64
}

// ShouldMerge decides whether adjacent vector ranges are below both cold floors.
func ShouldMerge(t MergeTrigger, left, right map[string]vector.Vector) bool {
	return t.SizeFloor > 0 && len(left)+len(right) <= t.SizeFloor && t.QPSFloor > 0 && t.LeftQPS <= t.QPSFloor && t.RightQPS <= t.QPSFloor
}

// MergeDescriptors preserves the surviving left group ID and rejects different placement:
// without identical replicas, replica movement must finish before a one-group handoff.
func MergeDescriptors(left, right Descriptor, mergedID uint64) (Descriptor, error) {
	if mergedID == 0 || left.End != right.Start {
		return Descriptor{}, errors.New("ann: ranges are not adjacent")
	}
	if len(left.Replicas) != len(right.Replicas) {
		return Descriptor{}, errors.New("ann: replica sets differ")
	}
	for i := range left.Replicas {
		if left.Replicas[i] != right.Replicas[i] {
			return Descriptor{}, errors.New("ann: replica sets differ")
		}
	}
	return Descriptor{ID: mergedID, Start: left.Start, End: right.End, Replicas: append([]raft.NodeID(nil), left.Replicas...)}, nil
}

// ExecuteLiveMerge re-proposes every frozen right-side vector into left's Raft group so
// the post-cutover descriptor names exactly one authoritative graph history.
func ExecuteLiveMerge(left, right Descriptor, rightVectors map[string]vector.Vector, target splitTarget, timeout time.Duration) (Descriptor, error) {
	merged, err := MergeDescriptors(left, right, left.ID)
	if err != nil {
		return Descriptor{}, err
	}
	for id, v := range rightVectors {
		if !right.Contains(id) {
			return Descriptor{}, fmt.Errorf("ann: merge source vector %q is outside the absorbed descriptor", id)
		}
		if err := insertAndConfirm(target, id, v, timeout); err != nil {
			return Descriptor{}, fmt.Errorf("ann: merge %q: %w", id, err)
		}
	}
	return merged, nil
}
