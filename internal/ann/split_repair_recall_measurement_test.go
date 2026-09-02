package ann

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/ashraf/consensa/internal/vector"
)

// TestMeasureRecallRepairVsRebuildAcrossRealisticIDSplit answers the question ADR-011
// left open and docs/adr/012-replicated-incremental-repair.md closes: does incremental
// repair (HNSW.Split/Repair, now replicated via ExecuteLiveSplitByRepair) actually recover
// any of the recall TestMeasureRecallAcrossRealisticIDSplit measured rebuild-from-scratch
// losing? Same dataset-generation parameters and RNG seed as that test (so the rebuild
// number here reproduces its 0.622 finding as a cross-check), with an added repair-based
// measurement using the identical split boundary and query set.
//
// This measures the in-memory HNSW.Split/Repair path directly rather than going through
// real Raft groups: ApplyMutation's "repair" case is proven deterministic and pure by
// TestExecuteLiveSplitByRepairMigratesRealVectors's bit-identical-replica check, so the
// recall a repaired child graph produces here is exactly what every replica of a real
// replicated split converges to -- there is nothing Raft-specific left to measure.
func TestMeasureRecallRepairVsRebuildAcrossRealisticIDSplit(t *testing.T) {
	const (
		numClusters   = 8
		perCluster    = 60
		dim           = 8
		k             = 10
		numQueries    = 50
		clusterSpread = 100.0
	)
	rng := rand.New(rand.NewPCG(7, 11))

	type point struct {
		id string
		v  vector.Vector
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
			id := fmt.Sprintf("%016x", rng.Uint64())
			points = append(points, point{id: id, v: v})
		}
	}

	all := map[string]vector.Vector{}
	for _, p := range points {
		all[p.id] = p.v
	}

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

	ids := make([]string, 0, len(all))
	for id := range all {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	splitKey := ids[len(ids)/2]
	leftOwns := func(id string) bool { return id < splitKey }

	leftData, rightData := map[string]vector.Vector{}, map[string]vector.Vector{}
	for id, v := range all {
		if leftOwns(id) {
			leftData[id] = v
		} else {
			rightData[id] = v
		}
	}

	route := func(q vector.Vector) (target string) {
		truth := bruteForceIDs(all, q, k)
		leftHits := 0
		for _, id := range truth {
			if leftData[id] != nil {
				leftHits++
			}
		}
		if leftHits*2 >= len(truth) {
			return "left"
		}
		return "right"
	}
	measurePostSplit := func(left, right *HNSW) float64 {
		var sum float64
		for _, q := range queries {
			truth := bruteForceIDs(all, q, k)
			truthSet := map[string]bool{}
			for _, id := range truth {
				truthSet[id] = true
			}
			target := left
			if route(q) == "right" {
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
			sum += float64(hits) / float64(k)
		}
		return sum / float64(len(queries))
	}

	// Rebuild-from-scratch: today's ExecuteLiveSplit strategy.
	buildRebuilt := func(data map[string]vector.Vector) *HNSW {
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
	rebuiltLeft, rebuiltRight := buildRebuilt(leftData), buildRebuilt(rightData)
	rebuildRecall := measurePostSplit(rebuiltLeft, rebuiltRight)

	// Incremental repair: the same operation ExecuteLiveSplitByRepair now replicates --
	// restore the parent's own graph, then drop everything outside each child's boundary
	// and re-trim affected neighbor lists, preserving the parent's existing edges among
	// retained nodes instead of discarding them.
	repairedLeft, repairedRight, err := parent.Split(leftOwns)
	if err != nil {
		t.Fatal(err)
	}
	repairRecall := measurePostSplit(repairedLeft, repairedRight)

	t.Logf("recall@%d before split (whole graph):        %.3f", k, baselineRecall)
	t.Logf("recall@%d after split, rebuild-from-scratch:  %.3f", k, rebuildRecall)
	t.Logf("recall@%d after split, incremental repair:    %.3f", k, repairRecall)
	t.Logf("repair vs rebuild delta: %+.3f (%+.1f%% relative to rebuild)", repairRecall-rebuildRecall, 100*(repairRecall-rebuildRecall)/rebuildRecall)
}
