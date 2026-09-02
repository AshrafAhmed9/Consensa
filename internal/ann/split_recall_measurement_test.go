package ann

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/ashraf/consensa/internal/vector"
)

// TestMeasureRecallAcrossRealisticIDSplit supplies the real, measured numbers
// docs/adr/011-vector-split-boundary.md cites: what recall@10 actually looks like
// immediately after a rebuild-from-scratch split (today's ExecuteLiveSplit behavior)
// when the split boundary is a lexicographic ID median with NO relationship to embedding
// space -- split_decision.go's own stated worst case, unlike TestLiveSplitPreservesSearchCorrectness's
// deliberately ID-aligned two-cluster dataset, which cannot exhibit the degradation this
// test is built to quantify.
//
// IDs here are random UUID-shaped strings assigned independently of each vector's
// position in a clustered embedding space, so the ID-lexicographic split point cuts
// through clusters arbitrarily -- the failure mode PLAN.md's Phase 12 names as the reason
// rebuild-from-scratch is not the final answer.
func TestMeasureRecallAcrossRealisticIDSplit(t *testing.T) {
	const (
		numClusters   = 8
		perCluster    = 60
		dim           = 8
		k             = 10
		numQueries    = 50
		clusterSpread = 100.0
	)
	rng := rand.New(rand.NewPCG(7, 11))

	// A clustered dataset (the realistic case: real embeddings cluster by semantic
	// similarity), but with IDs drawn independently of cluster membership -- unlike
	// durable_split_test.go's dataset, where ID prefix ("low"/"high") IS the cluster,
	// making its split boundary accidentally optimal. Real primary keys (UUIDs, hashes)
	// have no such relationship to embedding space, which is the case this measures.
	type point struct {
		id string
		v  vector.Vector
		c  int
	}
	var points []point
	centers := make([]vector.Vector, numClusters)
	for c := 0; c < numClusters; c++ {
		center := make(vector.Vector, dim)
		for d := 0; d < dim; d++ {
			center[d] = float32(rng.Float64()*clusterSpread*float64(numClusters)) - float32(clusterSpread*float64(numClusters)/2)
		}
		centers[c] = center
	}
	for c := 0; c < numClusters; c++ {
		for i := 0; i < perCluster; i++ {
			v := make(vector.Vector, dim)
			for d := 0; d < dim; d++ {
				v[d] = centers[c][d] + float32(rng.NormFloat64())
			}
			// Random hex ID, independent of c -- the realistic case.
			id := fmt.Sprintf("%016x", rng.Uint64())
			points = append(points, point{id: id, v: v, c: c})
		}
	}

	all := map[string]vector.Vector{}
	for _, p := range points {
		all[p.id] = p.v
	}

	// Pre-split baseline: recall@k against the whole (unsplit) graph.
	parent, err := NewHNSW(Config{Dimension: dim, M: 8, EFConstruction: 40, EFSearch: 40, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range points {
		if err := parent.Insert(p.id, p.v); err != nil {
			t.Fatal(err)
		}
	}
	queries := make([]vector.Vector, numQueries)
	for i := range queries {
		queries[i] = points[rng.IntN(len(points))].v
	}
	baselineRecall := measureRecall(t, parent, all, queries, k)

	// Split at the ID-lexicographic median -- exactly ShouldSplit's own boundary choice
	// (split_decision.go), not a clustering-aware one.
	ids := make([]string, 0, len(all))
	for id := range all {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	splitKey := ids[len(ids)/2]

	leftData, rightData := map[string]vector.Vector{}, map[string]vector.Vector{}
	for id, v := range all {
		if id < splitKey {
			leftData[id] = v
		} else {
			rightData[id] = v
		}
	}

	// Rebuild-from-scratch, matching ExecuteLiveSplit's actual strategy: a fresh HNSW per
	// child, built by reinserting exactly the vectors that child now owns.
	buildChild := func(data map[string]vector.Vector) *HNSW {
		h, err := NewHNSW(Config{Dimension: dim, M: 8, EFConstruction: 40, EFSearch: 40, Seed: 1})
		if err != nil {
			t.Fatal(err)
		}
		for id, v := range data {
			if err := h.Insert(id, v); err != nil {
				t.Fatal(err)
			}
		}
		return h
	}
	left := buildChild(leftData)
	right := buildChild(rightData)

	// Post-split recall: a query is answered by whichever child holds a majority of its
	// own true top-k (mirroring how a real router would send a query toward the child
	// covering the relevant keyspace region) -- but ground truth is still the FULL
	// pre-split dataset, since a real caller doesn't know results now live in two places.
	// This is exactly where a query whose true neighbors straddle the ID cut loses recall:
	// the child it's routed to may hold only some of them.
	var postSum float64
	for _, q := range queries {
		truth := bruteForceIDs(all, q, k)
		truthSet := map[string]bool{}
		for _, id := range truth {
			truthSet[id] = true
		}
		leftHits, rightHits := 0, 0
		for id := range truthSet {
			if _, ok := leftData[id]; ok {
				leftHits++
			} else {
				rightHits++
			}
		}
		var target *HNSW
		if leftHits >= rightHits {
			target = left
		} else {
			target = right
		}
		results, err := target.Search(q, k, 40)
		if err != nil {
			t.Fatal(err)
		}
		hits := 0
		for _, r := range results {
			if truthSet[r.ID] {
				hits++
			}
		}
		postSum += float64(hits) / float64(k)
	}
	postRecall := postSum / float64(numQueries)

	t.Logf("recall@%d before split (whole graph): %.3f", k, baselineRecall)
	t.Logf("recall@%d after ID-median split, routed to majority-owning child: %.3f", k, postRecall)
	t.Logf("recall dip: %.3f (%.1f%% relative)", baselineRecall-postRecall, 100*(baselineRecall-postRecall)/baselineRecall)

	// Not a correctness assertion on an exact number (the dip is inherent to ID-based
	// splitting and expected to be real) -- just a sanity bound so a future regression in
	// either HNSW or this measurement itself doesn't silently stop meaning anything.
	if baselineRecall < 0.8 {
		t.Fatalf("baseline recall too low to draw a conclusion from (%.3f) -- HNSW config or dataset changed", baselineRecall)
	}
}

func measureRecall(t *testing.T, h *HNSW, data map[string]vector.Vector, queries []vector.Vector, k int) float64 {
	t.Helper()
	var sum float64
	for _, q := range queries {
		truth := bruteForceIDs(data, q, k)
		truthSet := map[string]bool{}
		for _, id := range truth {
			truthSet[id] = true
		}
		results, err := h.Search(q, k, 40)
		if err != nil {
			t.Fatal(err)
		}
		hits := 0
		for _, r := range results {
			if truthSet[r.ID] {
				hits++
			}
		}
		sum += float64(hits) / float64(k)
	}
	return sum / float64(len(queries))
}

// bruteForceIDs is bruteForceTopK's dataset-and-signature twin (durable_split_test.go)
// but takes the whole dataset by value rather than reaching for package-level state, kept
// separate to avoid coupling this file to that test's helper naming.
func bruteForceIDs(data map[string]vector.Vector, query vector.Vector, k int) []string {
	type scored struct {
		id   string
		dist float64
	}
	all := make([]scored, 0, len(data))
	for id, v := range data {
		var sum float64
		for i := range v {
			d := float64(v[i]) - float64(query[i])
			sum += d * d
		}
		all = append(all, scored{id, sum})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].dist < all[j].dist })
	if k > len(all) {
		k = len(all)
	}
	out := make([]string, k)
	for i := 0; i < k; i++ {
		out[i] = all[i].id
	}
	return out
}
