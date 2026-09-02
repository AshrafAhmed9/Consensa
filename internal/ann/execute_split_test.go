package ann

import (
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/vector"
)

// waitForAnnLeader is ann's counterpart to kv's waitForLeader (internal/kv/execute_split_test.go).
func waitForAnnLeader(t *testing.T, live []*DurableNode, timeout time.Duration) *DurableNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range live {
			if _, _, isLeader := n.Status(); isLeader {
				return n
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("group never elected a leader")
	return nil
}

// executeAnnLiveSplitAnyLeader mirrors kv's executeLiveSplitAnyLeader exactly, for the
// identical reason: find each child's CURRENT leader first so ExecuteLiveSplit's own
// per-key retry only has to cover genuine transient leadership changes.
func executeAnnLiveSplitAnyLeader(t *testing.T, parentDescriptor Descriptor, parentVectors map[string]vector.Vector, splitKey string, leftID, rightID uint64, leftLive, rightLive []*DurableNode, overallTimeout time.Duration) (Descriptor, Descriptor) {
	t.Helper()
	deadline := time.Now().Add(overallTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		left := waitForAnnLeader(t, leftLive, overallTimeout)
		right := waitForAnnLeader(t, rightLive, overallTimeout)
		leftDesc, rightDesc, err := ExecuteLiveSplit(parentDescriptor, parentVectors, splitKey, leftID, rightID, left, right, 5*time.Second)
		if err == nil {
			return leftDesc, rightDesc
		}
		lastErr = err
	}
	t.Fatalf("ExecuteLiveSplit never succeeded within %s: %v", overallTimeout, lastErr)
	return Descriptor{}, Descriptor{}
}

// TestExecuteLiveSplitMigratesRealVectors proves the reusable library function (not
// durable_split_test.go's own hand-written migration loop) against real 3-node parent and
// child Raft groups: every vector lands in exactly the right child, value-identical to
// what the parent held, with none lost, duplicated, or leaking across the new boundary --
// the vector-plane counterpart of kv's TestExecuteLiveSplitMigratesRealData.
func TestExecuteLiveSplitMigratesRealVectors(t *testing.T) {
	cfg := Config{Dimension: 4, M: 4, EFConstruction: 20, EFSearch: 20, Seed: 3}

	const tick = 10 * time.Millisecond
	_, parentLive := newDurableGroupForSplit(t, 1, cfg)
	stopParent := make(chan struct{})
	wgParent := driveDurable(parentLive, tick, stopParent)

	leader := waitForAnnLeader(t, parentLive, 10*time.Second)

	dataset := map[string]vector.Vector{
		"a": {1, 0, 0, 0}, "b": {2, 0, 0, 0}, "n": {3, 0, 0, 0}, "z": {4, 0, 0, 0},
	}
	inserted := 0
	for id, v := range dataset {
		if err := leader.Insert(id, v); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
		inserted++
		for _, n := range parentLive {
			waitForApplied(t, n, inserted, 3*time.Second)
		}
	}
	parentVectors := leader.AllVectors()
	if len(parentVectors) != len(dataset) {
		t.Fatalf("parent has %d vectors, want %d", len(parentVectors), len(dataset))
	}

	parentDescriptor := Descriptor{ID: 1, Start: "", End: "", Replicas: []raft.NodeID{1, 2, 3}}
	splitKey := "n"

	close(stopParent)
	wgParent.Wait()

	_, leftLive := newDurableGroupForSplit(t, 11, cfg)
	_, rightLive := newDurableGroupForSplit(t, 21, cfg)
	stopChildren := make(chan struct{})
	wgChildren := driveDurable(append(append([]*DurableNode{}, leftLive...), rightLive...), tick, stopChildren)
	defer func() { close(stopChildren); wgChildren.Wait() }()

	leftDesc, _ := executeAnnLiveSplitAnyLeader(t, parentDescriptor, parentVectors, splitKey, 101, 102, leftLive, rightLive, 30*time.Second)

	wantLeft := map[string]vector.Vector{}
	wantRight := map[string]vector.Vector{}
	for id, v := range dataset {
		if leftDesc.Contains(id) {
			wantLeft[id] = v
		} else {
			wantRight[id] = v
		}
	}
	if len(wantLeft)+len(wantRight) != len(dataset) {
		t.Fatalf("descriptor boundary math is wrong: left=%d right=%d want %d total", len(wantLeft), len(wantRight), len(dataset))
	}

	checkChild := func(live []*DurableNode, want map[string]vector.Vector, label string) {
		t.Helper()
		checkDeadline := time.Now().Add(20 * time.Second)
		var got map[string]vector.Vector
		for {
			for _, n := range live {
				if all := n.AllVectors(); len(all) == len(want) {
					got = all
				}
			}
			if got != nil {
				break
			}
			if time.Now().After(checkDeadline) {
				t.Fatalf("%s: never converged to %d vectors", label, len(want))
			}
			time.Sleep(10 * time.Millisecond)
		}
		for id, v := range want {
			gv, ok := got[id]
			if !ok || !bytesEqualVector(gv, v) {
				t.Fatalf("%s: vector %q = %v, want %v", label, id, gv, v)
			}
		}
		for id := range got {
			if _, ok := want[id]; !ok {
				t.Fatalf("%s: unexpected vector %q leaked in from the wrong side of the split", label, id)
			}
		}
	}
	checkChild(leftLive, wantLeft, "left child")
	checkChild(rightLive, wantRight, "right child")
}
