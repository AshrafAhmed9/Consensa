package kv

import (
	"bytes"
	"errors"
	"sort"

	"github.com/ashraf/consensa/internal/raft"
)

// SplitTrigger names the criteria ShouldSplit checks. Both are optional (a non-positive
// threshold disables that criterion entirely); a range recommends splitting once EITHER
// one it has an active (positive) threshold for is exceeded, so a deployment that only
// cares about one signal doesn't have to reason about the other at all.
type SplitTrigger struct {
	// SizeThreshold, if positive, recommends a split once key count strictly exceeds it.
	SizeThreshold int
	// QPS is this range's own currently observed request rate (reads and writes both --
	// see cmd/consensa's own per-range counters for how it's measured), and QPSThreshold,
	// if positive, recommends a split once QPS strictly exceeds it. This is PLAN.md's own
	// named gap ("no QPS-based trigger exists, only size") -- a range can be reasonably
	// small by key count yet still be a genuine hot spot under skewed access patterns
	// (a handful of keys read far more often than the rest of the range), which
	// size-only splitting can never detect.
	QPS, QPSThreshold float64
}

// ShouldSplit decides whether a range holding keys should split under trigger's criteria,
// and if so, which key to split at. This is the piece PLAN.md's Phase 10 names as still
// missing -- "no automatic range-split trigger" -- and it deliberately closes only the
// DECISION, not execution: a caller still has to run the already-proven live-migration
// strategy (durable_split_test.go's TestLiveSplitPreservesKVCorrectness) against whatever
// key this returns, and this project has no dynamic descriptor/routing update path yet
// (Router and meta.go both operate on a fixed, static set of ranges) to actually cut real
// traffic over to two fresh groups automatically. Stated plainly rather than implied:
// this function alone does not perform a split.
//
// The split point is always the median key by sorted order, not by byte-value midpoint,
// regardless of WHICH criterion fired -- picking the middle of an arbitrary keyspace by
// value (e.g. bisecting between "a" and "z") can produce a wildly uneven split if the
// actual key distribution is skewed, while the median of the keys actually present always
// divides the range's real data in half. A QPS-triggered split still needs a real interior
// key to split at, and the median remains the right choice even when the reason for
// splitting was load rather than size: the goal either way is two roughly-equal-sized
// children, since a maximally uneven split would just relocate the same size problem
// entirely into one child.
func ShouldSplit(trigger SplitTrigger, keys map[string][]byte) ([]byte, bool) {
	sizeExceeded := trigger.SizeThreshold > 0 && len(keys) > trigger.SizeThreshold
	qpsExceeded := trigger.QPSThreshold > 0 && trigger.QPS > trigger.QPSThreshold
	if !sizeExceeded && !qpsExceeded {
		return nil, false
	}
	// A QPS-triggered split has no size guarantee behind it (unlike the size trigger,
	// where len(keys) > threshold >= 1 already implies at least 2 keys) -- a genuinely
	// hot single-key or empty range cannot be split into two non-empty children at all,
	// so this must be checked independently of which criterion fired.
	if len(keys) < 2 {
		return nil, false
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	// The key at the midpoint index becomes the new right child's Start (inclusive, per
	// Descriptor's own half-open [Start, End) convention) -- so it must not be sorted[0],
	// which would make the left child empty. len(keys) >= 2 (checked above) guarantees
	// mid >= 1, always a valid interior split point.
	mid := len(sorted) / 2
	return []byte(sorted[mid]), true
}

// SplitDescriptor divides one descriptor at splitKey without a gap or overlap. Callers
// persist the replacement atomically in metadata before exposing either child to routing.
func SplitDescriptor(parent Descriptor, splitKey []byte, leftID, rightID uint64) (Descriptor, Descriptor, error) {
	if !parent.Contains(splitKey) || bytes.Equal(splitKey, parent.Start) || (len(parent.End) > 0 && bytes.Equal(splitKey, parent.End)) {
		return Descriptor{}, Descriptor{}, errors.New("kv: split key outside parent interior")
	}
	if leftID == 0 || rightID == 0 || leftID == rightID {
		return Descriptor{}, Descriptor{}, errors.New("kv: invalid child IDs")
	}
	left := Descriptor{ID: leftID, Start: append([]byte(nil), parent.Start...), End: append([]byte(nil), splitKey...), Replicas: append([]raft.NodeID(nil), parent.Replicas...)}
	right := Descriptor{ID: rightID, Start: append([]byte(nil), splitKey...), End: append([]byte(nil), parent.End...), Replicas: append([]raft.NodeID(nil), parent.Replicas...)}
	return left, right, nil
}
