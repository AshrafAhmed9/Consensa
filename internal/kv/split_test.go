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
	if _, ok := ShouldSplit(3, keys); ok {
		t.Fatal("split triggered at exactly the threshold, want strictly above it")
	}
	if _, ok := ShouldSplit(4, keys); ok {
		t.Fatal("split triggered below threshold")
	}
	if _, ok := ShouldSplit(0, keys); ok {
		t.Fatal("split triggered with a non-positive threshold")
	}
	if _, ok := ShouldSplit(2, keys); !ok {
		t.Fatal("split did not trigger above threshold")
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
	splitKey, ok := ShouldSplit(3, keys)
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
