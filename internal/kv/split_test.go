package kv

import (
	"testing"

	"github.com/ashraf/consensa/internal/raft"
)

// TestShouldSplitRespectsThreshold proves ShouldSplit stays silent below threshold and
// only fires once the key count actually exceeds it -- an off-by-one here would either
// split a range that hasn't actually grown past the configured limit, or fail to trigger
// exactly at the boundary.
func TestShouldSplitRespectsThreshold(t *testing.T) {
	keys := map[string][]byte{"a": nil, "b": nil, "c": nil}
	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 3}, keys); ok {
		t.Fatal("split triggered at exactly the threshold, want strictly above it")
	}
	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 4}, keys); ok {
		t.Fatal("split triggered below threshold")
	}
	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 0}, keys); ok {
		t.Fatal("split triggered with a non-positive threshold")
	}
	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 2}, keys); !ok {
		t.Fatal("split did not trigger above threshold")
	}
}

// TestShouldSplitQPSTriggerIndependentOfSize proves the QPS criterion recommends a split
// on its own, even when the size criterion is disabled or would never fire -- a range can
// be small by key count yet genuinely hot under a skewed access pattern, which size-only
// splitting can never detect (PLAN.md's own named gap, "no QPS-based trigger exists, only
// size").
func TestShouldSplitQPSTriggerIndependentOfSize(t *testing.T) {
	keys := map[string][]byte{"a": nil, "b": nil}

	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 1000, QPS: 50, QPSThreshold: 100}, keys); ok {
		t.Fatal("split triggered with both criteria below their thresholds")
	}
	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 1000, QPS: 500, QPSThreshold: 100}, keys); !ok {
		t.Fatal("QPS above threshold did not trigger a split, even though size threshold (1000) was nowhere close")
	}
	// QPSThreshold <= 0 must disable the QPS criterion entirely, matching SizeThreshold's
	// own "non-positive disables" convention -- a huge QPS value must not leak through.
	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 1000, QPS: 1_000_000, QPSThreshold: 0}, keys); ok {
		t.Fatal("split triggered with QPSThreshold disabled (<= 0)")
	}
}

// TestShouldSplitRefusesSingleKeyEvenUnderQPSPressure proves a range that cannot produce
// two non-empty children never recommends a split, regardless of which criterion fired --
// a QPS-triggered split has no size guarantee behind it the way the size trigger's own
// "len(keys) > threshold >= 1" implication does, so this must be checked independently.
func TestShouldSplitRefusesSingleKeyEvenUnderQPSPressure(t *testing.T) {
	keys := map[string][]byte{"a": nil}
	if _, ok := ShouldSplit(SplitTrigger{QPS: 1_000_000, QPSThreshold: 1}, keys); ok {
		t.Fatal("split triggered on a single-key range, which cannot produce two non-empty children")
	}
}

// TestShouldSplitPicksAnInteriorMedianKey proves the returned key is always a real
// interior split point -- SplitDescriptor rejects Start/End themselves, so ShouldSplit
// picking either would make the whole pipeline fail downstream -- and that it divides the
// actual key distribution close to evenly, not just legally.
func TestShouldSplitPicksAnInteriorMedianKey(t *testing.T) {
	keys := map[string][]byte{}
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		keys[k] = []byte("v")
	}
	splitKey, ok := ShouldSplit(SplitTrigger{SizeThreshold: 3}, keys)
	if !ok {
		t.Fatal("expected a split to trigger")
	}
	if string(splitKey) == "a" {
		t.Fatal("split key must not be the smallest key (would leave the left child empty)")
	}
	left, right := 0, 0
	for k := range keys {
		if k < string(splitKey) {
			left++
		} else {
			right++
		}
	}
	if left == 0 || right == 0 {
		t.Fatalf("split key %q produced an empty child: left=%d right=%d", splitKey, left, right)
	}
	diff := left - right
	if diff < 0 {
		diff = -diff
	}
	if diff > 1 {
		t.Fatalf("split key %q divided keys unevenly: left=%d right=%d", splitKey, left, right)
	}
}

// TestSplitDescriptorPreservesExactOwnership proves every sample key remains in exactly one child.
func TestSplitDescriptorPreservesExactOwnership(t *testing.T) {
	p := Descriptor{ID: 1, Start: []byte("a"), End: []byte("z"), Replicas: []raft.NodeID{1, 2, 3}}
	l, r, err := SplitDescriptor(p, []byte("m"), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{[]byte("a"), []byte("l"), []byte("m"), []byte("y")} {
		if l.Contains(key) == r.Contains(key) {
			t.Fatalf("ownership broken for %q", key)
		}
	}
}
