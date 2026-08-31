package ann

import (
	"errors"
	"github.com/ashraf/consensa/internal/vector"
	"sort"
)

// IVFFlat partitions vectors by fixed centroids and scans the nearest lists exactly.
// It is a deliberately simple baseline for measuring HNSW's recall/latency trade-off.
type IVFFlat struct {
	dimension int
	centroids []vector.Vector
	lists     [][]ivfItem
}
type ivfItem struct {
	id string
	v  vector.Vector
}

// NewIVFFlat creates an index using externally chosen centroids, keeping k-means training
// deterministic and separately benchmarkable from online insert/search behaviour.
func NewIVFFlat(dimension int, centroids []vector.Vector) (*IVFFlat, error) {
	if dimension <= 0 || len(centroids) == 0 {
		return nil, errors.New("ann: invalid IVF config")
	}
	for _, c := range centroids {
		if err := c.ValidateDimension(dimension); err != nil {
			return nil, err
		}
	}
	out := &IVFFlat{dimension: dimension, centroids: centroids, lists: make([][]ivfItem, len(centroids))}
	return out, nil
}

// Insert places a vector into its closest inverted list.
func (i *IVFFlat) Insert(id string, v vector.Vector) error {
	if err := v.ValidateDimension(i.dimension); err != nil {
		return err
	}
	best := 0
	for c := 1; c < len(i.centroids); c++ {
		if vector.L2Squared(v, i.centroids[c]) < vector.L2Squared(v, i.centroids[best]) {
			best = c
		}
	}
	i.lists[best] = append(i.lists[best], ivfItem{id: id, v: append(vector.Vector(nil), v...)})
	return nil
}

// Search probes nProbe closest lists and returns exact ordering within the candidates.
func (i *IVFFlat) Search(q vector.Vector, k, nProbe int) ([]Result, error) {
	if err := q.ValidateDimension(i.dimension); err != nil {
		return nil, err
	}
	if k <= 0 {
		return nil, errors.New("ann: k must be positive")
	}
	if nProbe <= 0 {
		nProbe = 1
	}
	cs := make([]Result, len(i.centroids))
	for n, c := range i.centroids {
		cs[n] = Result{ID: string(rune(n)), Distance: vector.L2Squared(q, c)}
	}
	sortResults(cs)
	if nProbe > len(cs) {
		nProbe = len(cs)
	}
	var out []Result
	for _, c := range cs[:nProbe] {
		for _, item := range i.lists[int([]rune(c.ID)[0])] {
			out = append(out, Result{ID: item.id, Distance: vector.L2Squared(q, item.v)})
		}
	}
	sortResults(out)
	if len(out) > k {
		out = out[:k]
	}
	return out, nil
}

var _ = sort.Strings
