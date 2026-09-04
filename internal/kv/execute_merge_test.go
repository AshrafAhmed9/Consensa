package kv

import (
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/raft"
)

// TestExecuteLiveMergeConsolidatesTwoRealRaftGroups proves the right child is copied into
// the existing left group with exact bytes, so descriptor cutover can name one history
// without a keyspace gap or a newly-created replacement parent.
func TestExecuteLiveMergeConsolidatesTwoRealRaftGroups(t *testing.T) {
	leftLive := newDurableRangeGroupForSplit(t, 101)
	rightLive := newDurableRangeGroupForSplit(t, 102)
	stop := make(chan struct{})
	wg := driveRanges(append(leftLive, rightLive...), 10*time.Millisecond, stop)
	defer func() { close(stop); wg.Wait() }()

	leftLeader := waitForLeader(t, leftLive, 10*time.Second)
	rightLeader := waitForLeader(t, rightLive, 10*time.Second)
	if err := leftLeader.Put([]byte("a"), []byte("left")); err != nil {
		t.Fatal(err)
	}
	if err := rightLeader.Put([]byte("z"), []byte("right")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var rightData map[string][]byte
	for time.Now().Before(deadline) {
		if data, err := rightLeader.AllKeys(); err == nil && len(data) == 1 {
			rightData = data
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if rightData == nil {
		t.Fatal("right range never applied source data")
	}
	left := Descriptor{ID: 101, Start: nil, End: []byte("m"), Replicas: []raft.NodeID{1, 2, 3}}
	right := Descriptor{ID: 102, Start: []byte("m"), End: nil, Replicas: []raft.NodeID{1, 2, 3}}
	merged, err := ExecuteLiveMerge(left, right, rightData, leftLeader, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for time.Now().Before(deadline) {
		data, err := leftLeader.AllKeys()
		if err == nil && string(data["z"]) == "right" {
			if !merged.Contains([]byte("z")) {
				t.Fatal("merged descriptor excludes absorbed key")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("left group never applied absorbed right data")
}
