package ann

import (
	"sort"

	"github.com/ashraf/consensa/internal/vector"
)

// ShouldSplit decides whether a range holding vectors has grown past threshold, and if
// so, which vector ID to split at -- the vector-plane counterpart of kv.ShouldSplit
// (internal/kv/split.go), same median-of-what's-actually-present reasoning.
//
// The split point is the median ID by lexicographic order, NOT a boundary derived from
// the vectors' positions in embedding space. This is a real, stated limitation, not an
// oversight: an ID-ordered bisection has no relationship to which vectors are actually
// near each other, so a query whose true nearest neighbors straddle the chosen ID
// boundary can see degraded recall immediately after a split, until both children have
// enough of their own data for HNSW's own graph structure to compensate. PLAN.md's own
// Phase 10 section names the three real strategies for this (rebuild from scratch,
// incremental repair, serve-stale-parent) as deserving a dedicated ADR with measured
// recall impact -- this function is deliberately NOT that: it is the minimum viable
// decision needed to prove automatic split EXECUTION end-to-end (mirroring what the KV
// plane already has), postponing the actual clustering-aware boundary choice to that
// future, measured work rather than inventing one here without evidence.
func ShouldSplit(threshold int, vectors map[string]vector.Vector) (string, bool) {
	if threshold <= 0 || len(vectors) <= threshold {
		return "", false
	}
	sorted := make([]string, 0, len(vectors))
	for id := range vectors {
		sorted = append(sorted, id)
	}
	sort.Strings(sorted)
	// mid >= 1 is always a valid interior split point: len(vectors) > threshold >= 1
	// guarantees at least 2 IDs, so this never selects sorted[0] (which would make the
	// left child empty, since Descriptor.Contains treats Start as inclusive).
	mid := len(sorted) / 2
	return sorted[mid], true
}
