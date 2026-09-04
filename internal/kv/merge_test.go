package kv

import (
	"github.com/ashraf/consensa/internal/raft"
	"testing"
	"time"
)

// TestMergeDescriptorsPreservesCombinedOwnership proves a merge restores exactly the two child spans.
func TestMergeDescriptorsPreservesCombinedOwnership(t *testing.T) {
	replicas := []raft.NodeID{1, 2, 3}
	left := Descriptor{ID: 1, Start: []byte("a"), End: []byte("m"), Replicas: replicas}
	right := Descriptor{ID: 2, Start: []byte("m"), End: []byte("z"), Replicas: replicas}
	merged, e := MergeDescriptors(left, right, 3)
	if e != nil {
		t.Fatal(e)
	}
	for _, key := range [][]byte{[]byte("a"), []byte("m"), []byte("y")} {
		if !merged.Contains(key) {
			t.Fatalf("merged range lost %q", key)
		}
	}
}

// TestShouldMergeRequiresBothColdFloors proves a hot neighbour cannot be swallowed just
// because its adjacent peer is idle; that would recreate the hotspot Phase 12 split.
func TestShouldMergeRequiresBothColdFloors(t *testing.T) {
	left := map[string][]byte{"a": []byte("1")}
	right := map[string][]byte{"n": []byte("2")}
	if ShouldMerge(MergeTrigger{SizeFloor: 2, LeftQPS: 1, RightQPS: 10, QPSFloor: 5}, left, right) {
		t.Fatal("merge accepted a hot right neighbour")
	}
	if !ShouldMerge(MergeTrigger{SizeFloor: 2, LeftQPS: 1, RightQPS: 2, QPSFloor: 5}, left, right) {
		t.Fatal("merge rejected an adjacent cold pair at its size floor")
	}
}

type mergeTarget struct{ values map[string][]byte }

func (t *mergeTarget) Put(key, value []byte) error {
	t.values[string(key)] = append([]byte(nil), value...)
	return nil
}
func (t *mergeTarget) Get(key []byte) ([]byte, error) { return t.values[string(key)], nil }

// TestExecuteLiveMergeFoldsAbsorbedDataIntoLeft proves descriptor expansion happens only
// after every source key is visible in left's existing Raft-group abstraction.
func TestExecuteLiveMergeFoldsAbsorbedDataIntoLeft(t *testing.T) {
	replicas := []raft.NodeID{1, 2, 3}
	left := Descriptor{ID: 11, Start: nil, End: []byte("m"), Replicas: replicas}
	right := Descriptor{ID: 12, Start: []byte("m"), End: nil, Replicas: replicas}
	target := &mergeTarget{values: map[string][]byte{"a": []byte("1")}}
	merged, err := ExecuteLiveMerge(left, right, map[string][]byte{"z": []byte("2")}, target, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if merged.ID != left.ID || !merged.Contains([]byte("a")) || !merged.Contains([]byte("z")) {
		t.Fatalf("merged descriptor = %+v", merged)
	}
	if got := string(target.values["z"]); got != "2" {
		t.Fatalf("absorbed key = %q, want 2", got)
	}
}
