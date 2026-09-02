package kv

import (
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/raft"
)

// waitForLeader polls a live group's replicas until one reports itself as leader --
// election typically settles within a few hundred milliseconds at these test tick
// settings, so this is a bounded wait, not a busy-retry against an unknown target.
func waitForLeader(t *testing.T, live []*DurableRange, timeout time.Duration) *DurableRange {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, r := range live {
			if role, _ := r.Status(); role == raft.Leader {
				return r
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("group never elected a leader")
	return nil
}

// executeLiveSplitAnyLeader waits for each child group's real leader (elections can take
// a moment to settle, and leadership can also change again mid-migration -- a real
// deployment tolerates that by having every process retry independently; this test finds
// the CURRENT leader first so ExecuteLiveSplit's own per-key retry only has to cover
// genuine transient leadership changes, not a cold, unsettled election) and calls
// ExecuteLiveSplit against that pair with a generous per-key timeout, retrying against a
// freshly re-resolved leader if leadership moved during the attempt.
func executeLiveSplitAnyLeader(t *testing.T, parentDescriptor Descriptor, parentData map[string][]byte, splitKey []byte, leftID, rightID uint64, leftLive, rightLive []*DurableRange, overallTimeout time.Duration) (Descriptor, Descriptor) {
	t.Helper()
	deadline := time.Now().Add(overallTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		left := waitForLeader(t, leftLive, overallTimeout)
		right := waitForLeader(t, rightLive, overallTimeout)
		leftDesc, rightDesc, err := ExecuteLiveSplit(parentDescriptor, parentData, splitKey, leftID, rightID, left, right, 5*time.Second)
		if err == nil {
			return leftDesc, rightDesc
		}
		lastErr = err
	}
	t.Fatalf("ExecuteLiveSplit never succeeded within %s: %v", overallTimeout, lastErr)
	return Descriptor{}, Descriptor{}
}

// TestExecuteLiveSplitMigratesRealData proves the reusable library function (not a test's
// own duplicated migration loop, unlike TestLiveSplitPreservesKVCorrectness) against real
// 3-node parent and child Raft groups: every key lands in exactly the right child, byte-
// identical to what the parent held, with no key lost, duplicated, or leaking across the
// new boundary -- the same invariants TestLiveSplitPreservesKVCorrectness proves for the
// hand-written loop, now proven for the function cmd/consensa actually calls.
func TestExecuteLiveSplitMigratesRealData(t *testing.T) {
	parentLive := newDurableRangeGroupForSplit(t, 1)
	stopParent := make(chan struct{})
	wgParent := driveRanges(parentLive, 10*time.Millisecond, stopParent)

	var leader *DurableRange
	deadline := time.Now().Add(10 * time.Second)
	for leader == nil && time.Now().Before(deadline) {
		for _, r := range parentLive {
			if role, _ := r.Status(); role == raft.Leader {
				leader = r
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("parent group never elected a leader")
	}

	dataset := map[string]string{"a": "1", "b": "2", "n": "3", "z": "4"}
	for k, v := range dataset {
		if err := leader.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		if all, err := leader.AllKeys(); err == nil && len(all) == len(dataset) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("parent group never converged on the full dataset")
		}
		time.Sleep(10 * time.Millisecond)
	}
	parentData, err := leader.AllKeys()
	if err != nil {
		t.Fatalf("parent AllKeys: %v", err)
	}

	parentDescriptor := Descriptor{ID: 1, Start: nil, End: nil, Replicas: []raft.NodeID{1, 2, 3}}
	splitKey := []byte("n")

	close(stopParent)
	wgParent.Wait()

	leftLive := newDurableRangeGroupForSplit(t, 11)
	rightLive := newDurableRangeGroupForSplit(t, 21)
	stopChildren := make(chan struct{})
	wgChildren := driveRanges(append(append([]*DurableRange{}, leftLive...), rightLive...), 10*time.Millisecond, stopChildren)
	defer func() { close(stopChildren); wgChildren.Wait() }()

	leftDesc, _ := executeLiveSplitAnyLeader(t, parentDescriptor, parentData, splitKey, 101, 102, leftLive, rightLive, 30*time.Second)

	wantLeft := map[string]string{}
	wantRight := map[string]string{}
	for k, v := range dataset {
		if leftDesc.Contains([]byte(k)) {
			wantLeft[k] = v
		} else {
			wantRight[k] = v
		}
	}
	if len(wantLeft)+len(wantRight) != len(dataset) {
		t.Fatalf("descriptor boundary math is wrong: left=%d right=%d want %d total", len(wantLeft), len(wantRight), len(dataset))
	}

	checkChild := func(live []*DurableRange, want map[string]string, label string) {
		t.Helper()
		checkDeadline := time.Now().Add(20 * time.Second)
		var got map[string][]byte
		for {
			for _, r := range live {
				if all, err := r.AllKeys(); err == nil && len(all) == len(want) {
					got = all
				}
			}
			if got != nil {
				break
			}
			if time.Now().After(checkDeadline) {
				t.Fatalf("%s: never converged to %d keys", label, len(want))
			}
			time.Sleep(10 * time.Millisecond)
		}
		for k, v := range want {
			if string(got[k]) != v {
				t.Fatalf("%s: key %q = %q, want %q", label, k, got[k], v)
			}
		}
		for k := range got {
			if _, ok := want[k]; !ok {
				t.Fatalf("%s: unexpected key %q leaked in from the wrong side of the split", label, k)
			}
		}
	}
	checkChild(leftLive, wantLeft, "left child")
	checkChild(rightLive, wantRight, "right child")
}
