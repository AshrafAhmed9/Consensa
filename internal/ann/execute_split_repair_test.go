package ann

import (
	"reflect"
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/vector"
)

// executeAnnLiveSplitByRepairAnyLeader mirrors executeAnnLiveSplitAnyLeader
// (execute_split_test.go) exactly, for ExecuteLiveSplitByRepair instead.
func executeAnnLiveSplitByRepairAnyLeader(t *testing.T, parentDescriptor Descriptor, parentSnapshot []byte, parentVectors map[string]vector.Vector, splitKey string, leftID, rightID uint64, leftLive, rightLive []*DurableNode, overallTimeout time.Duration) (Descriptor, Descriptor) {
	t.Helper()
	deadline := time.Now().Add(overallTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		left := waitForAnnLeader(t, leftLive, overallTimeout)
		right := waitForAnnLeader(t, rightLive, overallTimeout)
		leftDesc, rightDesc, err := ExecuteLiveSplitByRepair(parentDescriptor, parentSnapshot, parentVectors, splitKey, leftID, rightID, left, right, 5*time.Second)
		if err == nil {
			return leftDesc, rightDesc
		}
		lastErr = err
	}
	t.Fatalf("ExecuteLiveSplitByRepair never succeeded within %s: %v", overallTimeout, lastErr)
	return Descriptor{}, Descriptor{}
}

// TestExecuteLiveSplitByRepairMigratesRealVectors is
// TestExecuteLiveSplitMigratesRealVectors's counterpart for the repair-based path: proves
// the same data-correctness invariants (every vector lands in exactly the right child,
// none lost/duplicated/leaked) against real 3-node Raft groups, AND additionally proves
// the property unique to this path -- every replica of a child group, having applied the
// identical single "repair" entry independently, ends up with a BIT-IDENTICAL graph
// (not just the same vectors: the same neighbor structure and entry point), confirming
// ApplyMutation's "repair" case really is the pure, deterministic function its own doc
// comment claims.
func TestExecuteLiveSplitByRepairMigratesRealVectors(t *testing.T) {
	cfg := Config{Dimension: 4, M: 4, EFConstruction: 20, EFSearch: 20, Seed: 3}

	const tick = 10 * time.Millisecond
	_, parentLive := newDurableGroupForSplit(t, 1, cfg)
	stopParent := make(chan struct{})
	wgParent := driveDurable(parentLive, tick, stopParent)

	leader := waitForAnnLeader(t, parentLive, 10*time.Second)

	dataset := map[string]vector.Vector{
		"a": {1, 0, 0, 0}, "b": {2, 0, 0, 0}, "c": {2.5, 0, 0, 0}, "n": {3, 0, 0, 0},
		"p": {3.5, 0, 0, 0}, "y": {4, 0, 0, 0}, "z": {4.5, 0, 0, 0},
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
	parentSnapshot, err := leader.Snapshot()
	if err != nil {
		t.Fatalf("parent snapshot: %v", err)
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

	leftDesc, _ := executeAnnLiveSplitByRepairAnyLeader(t, parentDescriptor, parentSnapshot, parentVectors, splitKey, 111, 121, leftLive, rightLive, 30*time.Second)

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

	checkChildData := func(live []*DurableNode, want map[string]vector.Vector, label string) {
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
	checkChildData(leftLive, wantLeft, "left child")
	checkChildData(rightLive, wantRight, "right child")

	// checkChildData above (mirroring TestExecuteLiveSplitMigratesRealVectors's own
	// helper) only waits for ANY ONE replica to reach the target count -- sufficient for
	// proving data correctness, but not for what checkChildIdentical below needs: a real
	// bug was found here (reproduced under -race, which slows scheduling enough to expose
	// it) where checkChildIdentical ran while two of three replicas had applied ZERO
	// entries yet (AppliedCount 1/0/0), so their empty graphs trivially "diverged" from
	// the one replica that had already applied the repair -- a test-timing bug, not a
	// nondeterminism in Repair itself. waitAllConverged closes that gap by requiring EVERY
	// replica to individually reach the target count before comparing snapshots.
	waitAllConverged := func(live []*DurableNode, want map[string]vector.Vector, label string) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for {
			done := true
			for _, n := range live {
				if len(n.AllVectors()) != len(want) {
					done = false
				}
			}
			if done {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: not every replica converged to %d vectors", label, len(want))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitAllConverged(leftLive, wantLeft, "left child")
	waitAllConverged(rightLive, wantRight, "right child")

	// Determinism: every replica of a child group must have applied the identical
	// "repair" entry to an identical bit-level result, not merely the same vector set --
	// this is what makes it safe for this operation to skip replicating the resulting
	// graph itself and rely purely on every replica computing it independently.
	checkChildIdentical := func(live []*DurableNode, label string) {
		t.Helper()
		var first []byte
		for _, n := range live {
			snap, err := n.Snapshot()
			if err != nil {
				t.Fatalf("%s: snapshot: %v", label, err)
			}
			if first == nil {
				first = snap
				continue
			}
			if !reflect.DeepEqual(snap, first) {
				t.Fatalf("%s: replica graphs diverged after repair -- not bit-identical", label)
			}
		}
	}
	checkChildIdentical(leftLive, "left child")
	checkChildIdentical(rightLive, "right child")
}
