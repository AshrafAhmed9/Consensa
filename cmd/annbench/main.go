// Command annbench builds an HNSW index from a dataset file, runs the dataset's queries
// against it, and prints the results as JSON.
//
// It does not compute recall itself. The Python script harness/bench/run_recall_benchmark.py
// does that, by comparing these results with its own exact brute-force answers. Keeping
// the two apart means the Go code is never trusted to mark its own homework.
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

// dataset is the input file, made by harness/bench/generate_dataset.py.
type dataset struct {
	Dimension int `json:"dimension"`
	Vectors   []struct {
		ID     string    `json:"id"`
		Values []float32 `json:"values"`
	} `json:"vectors"`
	Queries [][]float32 `json:"queries"`
}

// queryResult holds the IDs found for one query, nearest first.
type queryResult struct {
	QueryIndex int      `json:"query_index"`
	IDs        []string `json:"ids"`
}

// sweepResult is the outcome of running every query with one efSearch value.
type sweepResult struct {
	Param        int           `json:"param"` // the efSearch used
	MeanSearchUs float64       `json:"mean_search_us"`
	Results      []queryResult `json:"results"`
}

// output is the JSON printed at the end.
type output struct {
	BuildMs    float64       `json:"build_ms"`
	NumVectors int           `json:"num_vectors"`
	Sweeps     []sweepResult `json:"sweeps"`
}

func main() {
	datasetPath := flag.String("dataset", "", "path to a dataset JSON file (see harness/bench/generate_dataset.py)")
	k := flag.Int("k", 10, "number of neighbours to request per query")
	m := flag.Int("m", 16, "HNSW M")
	efConstruction := flag.Int("ef-construction", 64, "HNSW EFConstruction")
	sweep := flag.String("sweep", "16,32,64,128", "comma-separated efSearch values to measure")
	seed := flag.Uint64("seed", 1, "seed for HNSW level assignment")
	flag.Parse()

	if *datasetPath == "" {
		fatal(fmt.Errorf("-dataset is required"))
	}
	raw, err := os.ReadFile(*datasetPath)
	if err != nil {
		fatal(fmt.Errorf("reading dataset: %w", err))
	}
	var ds dataset
	if err := json.Unmarshal(raw, &ds); err != nil {
		fatal(fmt.Errorf("parsing dataset: %w", err))
	}
	efValues, err := parseIntList(*sweep)
	if err != nil {
		fatal(fmt.Errorf("-sweep: %w", err))
	}

	// Build the index and time how long it takes.
	buildStart := time.Now()
	index, err := ann.NewHNSW(ann.Config{Dimension: ds.Dimension, M: *m, EFConstruction: *efConstruction, EFSearch: efValues[0], Seed: *seed})
	if err != nil {
		fatal(err)
	}
	for _, v := range ds.Vectors {
		if err := index.Insert(v.ID, vector.Vector(v.Values)); err != nil {
			fatal(err)
		}
	}
	out := output{BuildMs: float64(time.Since(buildStart).Microseconds()) / 1000.0, NumVectors: len(ds.Vectors)}

	// For each efSearch value, run every query and time the searches.
	for _, ef := range efValues {
		var totalSearch time.Duration
		results := make([]queryResult, 0, len(ds.Queries))
		for qi, q := range ds.Queries {
			start := time.Now()
			hits, err := index.Search(vector.Vector(q), *k, ef)
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
		out.Sweeps = append(out.Sweeps, sweepResult{Param: ef, MeanSearchUs: meanUs, Results: results})
	}

	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		fatal(err)
	}
}

// parseIntList turns "16,32,64" into []int{16, 32, 64}.
func parseIntList(s string) ([]int, error) {
	var out []int
	for _, p := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, fmt.Errorf("malformed value %q: %w", p, err)
		}
		out = append(out, n)
	}
	return out, nil
}

// fatal prints the error and exits.
func fatal(err error) {
	fmt.Fprintf(os.Stderr, "annbench: %v\n", err)
	os.Exit(1)
}
