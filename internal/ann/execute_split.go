package ann

import (
	"fmt"
	"time"

	"github.com/ashraf/consensa/internal/vector"
)

// splitTarget is the subset of *DurableNode ExecuteLiveSplit needs from each child,
// declared narrowly for the same reason kv.splitTarget is (internal/kv/execute_split.go).
type splitTarget interface {
	Insert(id string, v vector.Vector) error
	GetVector(id string) (vector.Vector, bool)
}

// ExecuteLiveSplit performs a real live split of an already-running parent vector range
// into two fresh child ranges -- the vector-plane counterpart of kv.ExecuteLiveSplit
// (internal/kv/execute_split.go), same "rebuild from scratch" strategy and same reasoning
// for why: it re-proposes every vector into whichever child descriptor now owns it,
// confirming each insert is actually visible before moving to the next one, exactly like
// durable_split_test.go's TestLiveSplitPreservesSearchCorrectness proved by hand -- this
// factors that proof into reusable code instead of leaving it only in a test body.
//
// It deliberately does NOT reuse HNSW.Split (split.go): that primitive clones and repairs
// a graph already held in ONE process's memory, which has no way to propagate the result
// to the other replicas of a live Raft group. A live split spanning real, independently
// running processes has to go through Raft the same way any other mutation does -- Insert
// on each child, not a direct graph clone.
//
// It does NOT call Meta.Replace itself -- the caller decides exactly when the new
// topology becomes visible to routing, matching kv.ExecuteLiveSplit's own contract.
func ExecuteLiveSplit(parentDescriptor Descriptor, parentVectors map[string]vector.Vector, splitKey string, leftID, rightID uint64, left, right splitTarget, perKeyTimeout time.Duration) (leftDescriptor, rightDescriptor Descriptor, err error) {
	leftDescriptor, rightDescriptor, err = SplitDescriptor(parentDescriptor, splitKey, leftID, rightID)
	if err != nil {
		return Descriptor{}, Descriptor{}, err
	}
	for id, v := range parentVectors {
		var target splitTarget
		switch {
		case leftDescriptor.Contains(id):
			target = left
		case rightDescriptor.Contains(id):
			target = right
		default:
			return Descriptor{}, Descriptor{}, fmt.Errorf("ann: vector %q belongs to neither child descriptor -- split boundary gap", id)
		}
		if err := insertAndConfirm(target, id, v, perKeyTimeout); err != nil {
			return Descriptor{}, Descriptor{}, fmt.Errorf("migrating vector %q: %w", id, err)
		}
	}
	return leftDescriptor, rightDescriptor, nil
}

// insertAndConfirm proposes id/v against target, retrying past a "not leader" error until
// the insert is actually confirmed visible via GetVector, or perKeyTimeout elapses.
//
// GetVector is checked on every iteration, not only after a locally successful Insert,
// for the identical reason kv.putAndConfirm does this (internal/kv/execute_split.go's own
// doc comment, found there as a real CI failure this file avoids by construction): when
// multiple processes independently run ExecuteLiveSplit against their own local replica
// of the same child group, only whichever one is actually leader can ever make Insert
// succeed, but the value it commits still replicates to every other process's local
// replica -- including ones whose own Insert calls keep failing "not leader" for the
// group's entire lifetime.
func insertAndConfirm(target splitTarget, id string, v vector.Vector, perKeyTimeout time.Duration) error {
	deadline := time.Now().Add(perKeyTimeout)
	var lastErr error
	for {
		if err := target.Insert(id, v); err != nil {
			lastErr = err
		}
		if got, ok := target.GetVector(id); ok && bytesEqualVector(got, v) {
			return nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return lastErr
			}
			return fmt.Errorf("ann: insert of %q did not become visible before the deadline", id)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// bytesEqualVector compares two vectors element-wise -- vector.Vector has no exported
// equality method, and this must be an exact-value check (not a distance threshold):
// insertAndConfirm needs to know THIS EXACT insert landed, not merely that some
// close-enough value is present.
func bytesEqualVector(a, b vector.Vector) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
