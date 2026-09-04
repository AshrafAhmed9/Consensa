package ann

import (
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/vector"
)

// TestShouldMergeRequiresBothColdFloors proves vector ranges only consolidate after both
// sides are cold, avoiding an immediate reversal of the split's load-isolation benefit.
func TestShouldMergeRequiresBothColdFloors(t *testing.T) {
	left := map[string]vector.Vector{"a": {1}}
	right := map[string]vector.Vector{"n": {2}}
	if ShouldMerge(MergeTrigger{SizeFloor: 2, LeftQPS: 1, RightQPS: 10, QPSFloor: 5}, left, right) {
		t.Fatal("merge accepted a hot neighbour")
	}
	if !ShouldMerge(MergeTrigger{SizeFloor: 2, LeftQPS: 1, RightQPS: 2, QPSFloor: 5}, left, right) {
		t.Fatal("merge rejected a cold pair")
	}
}

type annMergeTarget struct{ values map[string]vector.Vector }

func (t *annMergeTarget) Insert(id string, v vector.Vector) error {
	t.values[id] = append(vector.Vector(nil), v...)
	return nil
}
func (t *annMergeTarget) GetVector(id string) (vector.Vector, bool) {
	v, ok := t.values[id]
	return v, ok
}

// TestExecuteLiveMergeFoldsAbsorbedVectorsIntoLeft proves the merged descriptor retains
// the left group identity only after every right-side vector is visible there.
func TestExecuteLiveMergeFoldsAbsorbedVectorsIntoLeft(t *testing.T) {
	replicas := []raft.NodeID{1, 2, 3}
	left := Descriptor{ID: 101, Start: "", End: "m", Replicas: replicas}
	right := Descriptor{ID: 102, Start: "m", End: "", Replicas: replicas}
	target := &annMergeTarget{values: map[string]vector.Vector{"a": {1}}}
	merged, err := ExecuteLiveMerge(left, right, map[string]vector.Vector{"z": {2}}, target, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if merged.ID != left.ID || !merged.Contains("a") || !merged.Contains("z") {
		t.Fatalf("merged descriptor = %+v", merged)
	}
	if got := target.values["z"]; len(got) != 1 || got[0] != 2 {
		t.Fatalf("absorbed vector = %v, want [2]", got)
	}
}
