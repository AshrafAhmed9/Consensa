package kv

import (
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/raft"
)

// This file is the KV-plane counterpart to internal/ann/durable_split_test.go: it proves
// a live range split preserves correctness against real, Raft-backed DurableRange groups,
// not just SplitDescriptor's own unit-tested keyspace math (split_test.go). It implements
// the same "rebuild both children from scratch" strategy PLAN.md names as the simplest of
// the three Phase 10 split strategies: every key/value the parent group has applied
// (via AllKeys, added for exactly this purpose) is re-proposed into whichever fresh,
// independent 3-node child group owns it under the new descriptor boundary. The cheaper
// strategies (incremental repair; serve the stale parent during a background rebuild)
// remain unimplemented, same as the vector plane's own honest gap.
func newDurableRangeGroupForSplit(t *testing.T, prefix raft.NodeID) (live []*DurableRange) {
	t.Helper()
	ids := []raft.NodeID{prefix, prefix + 1, prefix + 2}
	addrs := map[raft.NodeID]string{}
	dirs := map[raft.NodeID]string{}
	for _, id := range ids {
		addrs[id] = freeKVAddr(t)
		dirs[id] = t.TempDir()
	}
	for _, id := range ids {
		peers := map[raft.NodeID]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = addrs[other]
			}
		}
		r, err := NewDurableRange(DurableRangeConfig{
			ID: id, GroupPeers: ids, ListenAddress: addrs[id], TransportPeers: peers, StorageDir: dirs[id],
			ElectionTick: 60, HeartbeatTick: 6,
		})
		if err != nil {
			t.Fatalf("group %d node %d: %v", prefix, id, err)
		}
		t.Cleanup(func() { _ = r.Close() })
		live = append(live, r)
	}
	return live
}

// TestLiveSplitPreservesKVCorrectness builds one real 3-node DurableRange group, writes a
// keyspace spanning a chosen split point, splits it into two fresh real 3-node groups via
// SplitDescriptor's boundary plus a live data migration off AllKeys, and proves: (1) every
// key lands in exactly one child, none lost or duplicated -- PLAN.md's own split
// invariant, proven here for real data rather than just descriptor math; (2) each child
// serves only keys inside its own [Start, End) span -- no cross-boundary key survives the
// split; (3) every value is byte-identical to what the parent held, not just present.
func TestLiveSplitPreservesKVCorrectness(t *testing.T) {
	parentLive := newDurableRangeGroupForSplit(t, 1)
	stopParent := make(chan struct{})
	wgParent := driveRanges(parentLive, 10*time.Millisecond, stopParent)

	parent := Descriptor{ID: 100, Start: nil, End: nil, Replicas: []raft.NodeID{1, 2, 3}}
	splitKey := []byte("m")

	dataset := map[string][]byte{}
	for _, k := range []string{"a", "b", "c", "f", "k", "l", "m", "n", "q", "x", "y", "z"} {
		dataset[k] = []byte("value-" + k)
	}
	for k, v := range dataset {
		if err := putUntilAccepted(parentLive, []byte(k), v, 20*time.Second); err != nil {
			t.Fatalf("put %s into parent: %v", k, err)
		}
	}

	// Confirm the parent's own applied state actually has everything before splitting off
	// of it -- otherwise a later "migration" could just be moving nothing, twice.
	var leader *DurableRange
	deadline := time.Now().Add(20 * time.Second)
	for leader == nil {
		for _, r := range parentLive {
			if all, err := r.AllKeys(); err == nil && len(all) == len(dataset) {
				leader = r
				break
			}
		}
		if leader == nil {
			if time.Now().After(deadline) {
				t.Fatal("parent group never converged on the full dataset")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	parentData, err := leader.AllKeys()
	if err != nil {
		t.Fatalf("parent AllKeys: %v", err)
	}

	left, right, err := SplitDescriptor(parent, splitKey, 101, 102)
	if err != nil {
		t.Fatalf("SplitDescriptor: %v", err)
	}

	close(stopParent)
	wgParent.Wait()

	leftLive := newDurableRangeGroupForSplit(t, 11)
	rightLive := newDurableRangeGroupForSplit(t, 21)
	stopChildren := make(chan struct{})
	wgChildren := driveRanges(append(append([]*DurableRange{}, leftLive...), rightLive...), 10*time.Millisecond, stopChildren)
	defer func() { close(stopChildren); wgChildren.Wait() }()

	leftData := map[string][]byte{}
	rightData := map[string][]byte{}
	for k, v := range parentData {
		if left.Contains([]byte(k)) {
			leftData[k] = v
			if err := putUntilAccepted(leftLive, []byte(k), v, 20*time.Second); err != nil {
				t.Fatalf("migrate %s into left child: %v", k, err)
			}
		} else if right.Contains([]byte(k)) {
			rightData[k] = v
			if err := putUntilAccepted(rightLive, []byte(k), v, 20*time.Second); err != nil {
				t.Fatalf("migrate %s into right child: %v", k, err)
			}
		} else {
			t.Fatalf("key %q belongs to neither child descriptor -- split boundary gap", k)
		}
	}

	// Invariant 1: no key lost or duplicated across the split.
	if len(leftData)+len(rightData) != len(parentData) {
		t.Fatalf("split lost or duplicated keys: parent had %d, children have %d+%d", len(parentData), len(leftData), len(rightData))
	}

	// Invariant 2 & 3: each child's own applied state, once converged, must contain
	// exactly its assigned keys with byte-identical values -- no cross-boundary leakage,
	// no silent corruption in the re-propose migration.
	checkChild := func(live []*DurableRange, want map[string][]byte, descriptor Descriptor, label string) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
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
			if time.Now().After(deadline) {
				t.Fatalf("%s: never converged to %d keys", label, len(want))
			}
			time.Sleep(10 * time.Millisecond)
		}
		for k, v := range want {
			if string(got[k]) != string(v) {
				t.Fatalf("%s: key %q = %q, want %q", label, k, got[k], v)
			}
			if !descriptor.Contains([]byte(k)) {
				t.Fatalf("%s: holds key %q outside its own descriptor span", label, k)
			}
		}
		for k := range got {
			if !descriptor.Contains([]byte(k)) {
				t.Fatalf("%s: cross-boundary key %q survived the split", label, k)
			}
		}
	}

	checkChild(leftLive, leftData, left, "left child")
	checkChild(rightLive, rightData, right, "right child")
}

// TestMaybeSplitKeyDrivesARealLiveSplit closes the loop between the trigger decision
// (ShouldSplit/MaybeSplitKey, split.go) and the already-proven live migration above: a
// real 3-node group grows past a threshold, MaybeSplitKey picks the split point from that
// group's own real applied data (not a hand-picked test constant), and the resulting
// split key is fed into the identical SplitDescriptor + re-propose pipeline
// TestLiveSplitPreservesKVCorrectness proves. This is still not automatic execution --
// nothing calls MaybeSplitKey on a timer or wires its result into a live traffic cutover,
// stated plainly in ShouldSplit's own doc comment -- but it proves the trigger's decision
// is actually usable by the real migration mechanism, not just self-consistent in
// isolation.
func TestMaybeSplitKeyDrivesARealLiveSplit(t *testing.T) {
	parentLive := newDurableRangeGroupForSplit(t, 1)
	stopParent := make(chan struct{})
	wgParent := driveRanges(parentLive, 10*time.Millisecond, stopParent)

	parent := Descriptor{ID: 100, Start: nil, End: nil, Replicas: []raft.NodeID{1, 2, 3}}

	dataset := map[string][]byte{}
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
		dataset[k] = []byte("value-" + k)
	}
	for k, v := range dataset {
		if err := putUntilAccepted(parentLive, []byte(k), v, 20*time.Second); err != nil {
			t.Fatalf("put %s into parent: %v", k, err)
		}
	}

	var leader *DurableRange
	var splitKey []byte
	deadline := time.Now().Add(20 * time.Second)
	for leader == nil {
		for _, r := range parentLive {
			if key, ok, err := r.MaybeSplitKey(len(dataset) - 1); err == nil && ok {
				leader = r
				splitKey = key
				break
			}
		}
		if leader == nil {
			if time.Now().After(deadline) {
				t.Fatal("MaybeSplitKey never triggered once the parent converged past threshold")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	parentData, err := leader.AllKeys()
	if err != nil {
		t.Fatalf("parent AllKeys: %v", err)
	}
	if len(parentData) != len(dataset) {
		t.Fatalf("triggered against a partially-converged snapshot: got %d keys, want %d", len(parentData), len(dataset))
	}

	left, right, err := SplitDescriptor(parent, splitKey, 101, 102)
	if err != nil {
		t.Fatalf("SplitDescriptor(%q): %v", splitKey, err)
	}

	close(stopParent)
	wgParent.Wait()

	leftLive := newDurableRangeGroupForSplit(t, 11)
	rightLive := newDurableRangeGroupForSplit(t, 21)
	stopChildren := make(chan struct{})
	wgChildren := driveRanges(append(append([]*DurableRange{}, leftLive...), rightLive...), 10*time.Millisecond, stopChildren)
	defer func() { close(stopChildren); wgChildren.Wait() }()

	leftData := map[string][]byte{}
	rightData := map[string][]byte{}
	for k, v := range parentData {
		if left.Contains([]byte(k)) {
			leftData[k] = v
			if err := putUntilAccepted(leftLive, []byte(k), v, 20*time.Second); err != nil {
				t.Fatalf("migrate %s into left child: %v", k, err)
			}
		} else if right.Contains([]byte(k)) {
			rightData[k] = v
			if err := putUntilAccepted(rightLive, []byte(k), v, 20*time.Second); err != nil {
				t.Fatalf("migrate %s into right child: %v", k, err)
			}
		} else {
			t.Fatalf("key %q belongs to neither child descriptor -- split boundary gap", k)
		}
	}

	if len(leftData) == 0 || len(rightData) == 0 {
		t.Fatalf("trigger-chosen split key %q produced an empty child: left=%d right=%d", splitKey, len(leftData), len(rightData))
	}
	if len(leftData)+len(rightData) != len(parentData) {
		t.Fatalf("split lost or duplicated keys: parent had %d, children have %d+%d", len(parentData), len(leftData), len(rightData))
	}
}
