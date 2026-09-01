// Command annbench builds a real ann.HNSW or ann.IVFFlat index over a dataset and reports
// its search results as JSON, so an independent process (the Python recall harness) can
// compute recall@k against its own brute-force ground truth without trusting anything
// computed inside this binary. This is the connector docs/benchmarks/04-ann.md names as
// missing: "must not be compared to real-corpus ANN benchmarks until the pinned-corpus
// harness is connected to the Go index."
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ashraf/consensa/internal/ann"
	"github.com/ashraf/consensa/internal/vector"
)

type datasetVector struct {
	ID     string    `json:"id"`
	Values []float32 `json:"values"`
}

type dataset struct {
	Dimension int             `json:"dimension"`
	Vectors   []datasetVector `json:"vectors"`
	Queries   [][]float32     `json:"queries"`
}

type queryResult struct {
	QueryIndex int      `json:"query_index"`
	IDs        []string `json:"ids"`
}

type sweepResult struct {
	Param        int           `json:"param"` // efSearch for HNSW, nProbe for IVFFlat
	MeanSearchUs float64       `json:"mean_search_us"`
	Results      []queryResult `json:"results"`
}

type output struct {
	IndexKind  string        `json:"index_kind"`
	BuildMs    float64       `json:"build_ms"`
	NumVectors int           `json:"num_vectors"`
	Sweeps     []sweepResult `json:"sweeps"`
}

// searcher is the narrow contract this tool needs from either real index type -- it
// deliberately does not import internal/server.Index, since that interface also carries
// Insert/Delete/Validate that this read-only benchmark tool has no use for.
type searcher interface {
	Search(vector.Vector, int, int) ([]ann.Result, error)
}

func main() {
	datasetPath := flag.String("dataset", "", "path to a dataset JSON file (see harness/bench/generate_dataset.py)")
	indexKind := flag.String("index", "hnsw", `"hnsw" or "ivfflat"`)
	k := flag.Int("k", 10, "number of neighbours to request per query")
	m := flag.Int("m", 16, "HNSW M (ignored for ivfflat)")
	efConstruction := flag.Int("ef-construction", 64, "HNSW EFConstruction (ignored for ivfflat)")
	sweep := flag.String("sweep", "16,32,64,128", "comma-separated efSearch (hnsw) or nProbe (ivfflat) values to measure")
	seed := flag.Uint64("seed", 1, "index construction seed (HNSW level assignment)")
	centroids := flag.Int("centroids", 16, "ivfflat only: number of centroids, taken as the first N dataset vectors (not k-means-trained -- see docs/benchmarks/04-ann.md)")
	flag.Parse()

	if *datasetPath == "" {
		fmt.Fprintln(os.Stderr, "annbench: -dataset is required")
		os.Exit(1)
	}
	raw, err := os.ReadFile(*datasetPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "annbench: reading dataset: %v\n", err)
		os.Exit(1)
	}
	var ds dataset
	if err := json.Unmarshal(raw, &ds); err != nil {
		fmt.Fprintf(os.Stderr, "annbench: parsing dataset: %v\n", err)
		os.Exit(1)
	}

	sweepValues, err := parseIntList(*sweep)
	if err != nil {
		fmt.Fprintf(os.Stderr, "annbench: -sweep: %v\n", err)
		os.Exit(1)
	}

	var index searcher
	var buildStart = time.Now()
	switch *indexKind {
	case "hnsw":
		h, err := ann.NewHNSW(ann.Config{Dimension: ds.Dimension, M: *m, EFConstruction: *efConstruction, EFSearch: sweepValues[0], Seed: *seed})
		if err != nil {
			fatal(err)
		}
		for _, v := range ds.Vectors {
			if err := h.Insert(v.ID, vector.Vector(v.Values)); err != nil {
				fatal(err)
			}
		}
		index = h
	case "ivfflat":
		n := *centroids
		if n > len(ds.Vectors) {
			n = len(ds.Vectors)
		}
		seedVectors := make([]vector.Vector, n)
		for i := 0; i < n; i++ {
			seedVectors[i] = vector.Vector(ds.Vectors[i].Values)
		}
		f, err := ann.NewIVFFlat(ds.Dimension, seedVectors)
		if err != nil {
			fatal(err)
		}
		for _, v := range ds.Vectors {
			if err := f.Insert(v.ID, vector.Vector(v.Values)); err != nil {
				fatal(err)
			}
		}
		index = f
	default:
		fmt.Fprintf(os.Stderr, "annbench: unknown -index %q\n", *indexKind)
		os.Exit(1)
	}
	buildMs := float64(time.Since(buildStart).Microseconds()) / 1000.0

	out := output{IndexKind: *indexKind, BuildMs: buildMs, NumVectors: len(ds.Vectors)}
	for _, param := range sweepValues {
		var totalSearch time.Duration
		results := make([]queryResult, 0, len(ds.Queries))
		for qi, q := range ds.Queries {
			start := time.Now()
			hits, err := index.Search(vector.Vector(q), *k, param)
			totalSearch += time.Since(start)
			if err != nil {
				fatal(err)
			}
			ids := make([]string, len(hits))
			for i, h := range hits {
				ids[i] = h.ID
			}
			results = append(results, queryResult{QueryIndex: qi, IDs: ids})
		}
		meanUs := 0.0
		if len(ds.Queries) > 0 {
			meanUs = float64(totalSearch.Microseconds()) / float64(len(ds.Queries))
		}
		out.Sweeps = append(out.Sweeps, sweepResult{Param: param, MeanSearchUs: meanUs, Results: results})
	}

	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		fatal(err)
	}
}

func parseIntList(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, fmt.Errorf("malformed value %q: %w", p, err)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("must name at least one value")
	}
	return out, nil
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "annbench: %v\n", err)
	os.Exit(1)
}
