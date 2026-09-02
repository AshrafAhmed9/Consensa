package ann

import (
	"sort"

	"github.com/ashraf/consensa/internal/vector"
)

// SplitTrigger mirrors kv.SplitTrigger (internal/kv/split.go) exactly, for the identical
// reason: both size and QPS criteria are optional (a non-positive threshold disables
// that one), and a range recommends splitting once EITHER active criterion is exceeded.
type SplitTrigger struct {
	// SizeThreshold, if positive, recommends a split once vector count strictly exceeds it.
	SizeThreshold int
	// QPS is this range's own currently observed request rate, and QPSThreshold, if
	// positive, recommends a split once QPS strictly exceeds it -- PLAN.md's own named
	// gap ("no QPS-based trigger exists, only size"), closed identically on both planes.
	QPS, QPSThreshold float64
}

// ShouldSplit decides whether a range holding vectors should split under trigger's
// criteria, and if so, which vector ID to split at -- the vector-plane counterpart of
// kv.ShouldSplit (internal/kv/split.go), same median-of-what's-actually-present reasoning
// and the same "either active criterion" combination.
//
// The split point is always the median ID by lexicographic order, NOT a boundary derived
// from the vectors' positions in embedding space, regardless of which criterion fired.
// This is a real, stated limitation, not an oversight: an ID-ordered bisection has no
// relationship to which vectors are actually near each other, so a query whose true
// nearest neighbors straddle the chosen ID boundary can see degraded recall immediately
// after a split, until both children have enough of their own data for HNSW's own graph
// structure to compensate. PLAN.md's own Phase 10 section names the three real strategies
// for this (rebuild from scratch, incremental repair, serve-stale-parent) as deserving a
// dedicated ADR with measured recall impact -- this function is deliberately NOT that: it
// is the minimum viable decision needed to prove automatic split EXECUTION end-to-end,
// postponing the actual clustering-aware boundary choice to that future, measured work
// rather than inventing one here without evidence.
func ShouldSplit(trigger SplitTrigger, vectors map[string]vector.Vector) (string, bool) {
	sizeExceeded := trigger.SizeThreshold > 0 && len(vectors) > trigger.SizeThreshold
	qpsExceeded := trigger.QPSThreshold > 0 && trigger.QPS > trigger.QPSThreshold
	if !sizeExceeded && !qpsExceeded {
		return "", false
	}
	// A QPS-triggered split has no size guarantee behind it (unlike the size trigger,
	// where len(vectors) > threshold >= 1 already implies at least 2 IDs) -- a genuinely
	// hot single-vector or empty range cannot be split into two non-empty children at
	// all, so this must be checked independently of which criterion fired.
	if len(vectors) < 2 {
		return "", false
	}
	sorted := make([]string, 0, len(vectors))
	for id := range vectors {
		sorted = append(sorted, id)
	}
	sort.Strings(sorted)
	// mid >= 1 is always a valid interior split point: len(vectors) >= 2 (checked above)
	// guarantees this never selects sorted[0] (which would make the left child empty,
	// since Descriptor.Contains treats Start as inclusive).
	mid := len(sorted) / 2
	return sorted[mid], true
}
