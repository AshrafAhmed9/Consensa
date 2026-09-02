package ann

import (
	"testing"

	"github.com/ashraf/consensa/internal/vector"
)

// TestShouldSplitRespectsThreshold mirrors kv.TestShouldSplitRespectsThreshold
// (internal/kv/split_test.go): stays silent below threshold, fires strictly above it.
func TestShouldSplitRespectsThreshold(t *testing.T) {
	vectors := map[string]vector.Vector{"a": {0}, "b": {1}, "c": {2}}
	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 3}, vectors); ok {
		t.Fatal("split triggered at exactly the threshold, want strictly above it")
	}
	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 4}, vectors); ok {
		t.Fatal("split triggered below threshold")
	}
	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 0}, vectors); ok {
		t.Fatal("split triggered with a non-positive threshold")
	}
	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 2}, vectors); !ok {
		t.Fatal("split did not trigger above threshold")
	}
}

// TestShouldSplitQPSTriggerIndependentOfSize mirrors kv's own test of the same name --
// the vector plane's QPS criterion must fire independently of key count too.
func TestShouldSplitQPSTriggerIndependentOfSize(t *testing.T) {
	vectors := map[string]vector.Vector{"a": {0}, "b": {1}}

	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 1000, QPS: 50, QPSThreshold: 100}, vectors); ok {
		t.Fatal("split triggered with both criteria below their thresholds")
	}
	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 1000, QPS: 500, QPSThreshold: 100}, vectors); !ok {
		t.Fatal("QPS above threshold did not trigger a split, even though size threshold (1000) was nowhere close")
	}
	if _, ok := ShouldSplit(SplitTrigger{SizeThreshold: 1000, QPS: 1_000_000, QPSThreshold: 0}, vectors); ok {
		t.Fatal("split triggered with QPSThreshold disabled (<= 0)")
	}
}

// TestShouldSplitRefusesSingleVectorEvenUnderQPSPressure mirrors kv's single-key
// guard test: a range that cannot produce two non-empty children never recommends a
// split, regardless of which criterion fired.
func TestShouldSplitRefusesSingleVectorEvenUnderQPSPressure(t *testing.T) {
	vectors := map[string]vector.Vector{"a": {0}}
	if _, ok := ShouldSplit(SplitTrigger{QPS: 1_000_000, QPSThreshold: 1}, vectors); ok {
		t.Fatal("split triggered on a single-vector range, which cannot produce two non-empty children")
	}
}

// TestShouldSplitPicksAnInteriorMedianID mirrors kv.TestShouldSplitPicksAnInteriorMedianKey.
func TestShouldSplitPicksAnInteriorMedianID(t *testing.T) {
	vectors := map[string]vector.Vector{}
	for _, id := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		vectors[id] = vector.Vector{0}
	}
	splitKey, ok := ShouldSplit(SplitTrigger{SizeThreshold: 3}, vectors)
	if !ok {
		t.Fatal("expected a split to trigger")
	}
	if splitKey == "a" {
		t.Fatal("split key must not be the smallest ID (would leave the left child empty)")
	}
	left, right := 0, 0
	for id := range vectors {
		if id < splitKey {
			left++
		} else {
			right++
		}
	}
	if left == 0 || right == 0 {
		t.Fatalf("split key %q produced an empty child: left=%d right=%d", splitKey, left, right)
	}
}
