package ann

import (
	"errors"
	"github.com/ashraf/consensa/internal/vector"
	"math"
	"math/rand/v2"
	"sort"
)

// Config controls HNSW graph density and search breadth.
type Config struct {
	Dimension, M, EFConstruction, EFSearch int
	Seed                                   uint64
}

// Result is one approximate neighbour ordered by increasing L2 distance.
type Result struct {
	ID       string
	Distance float32
}
type node struct {
	id        string
	v         vector.Vector
	level     int
	neighbors map[int][]string
}

// HNSW is a single-writer proximity graph. Deterministic level generation and sorting by
// ID break distance ties identically on every replica, which makes Raft mutation logging viable.
type HNSW struct {
	cfg      Config
	rng      *rand.Rand
	nodes    map[string]*node
	entry    string
	maxLevel int
}

// NewHNSW validates immutable index parameters.
func NewHNSW(c Config) (*HNSW, error) {
	if c.Dimension <= 0 || c.M <= 0 || c.EFConstruction < c.M || c.EFSearch <= 0 {
		return nil, errors.New("ann: invalid HNSW config")
	}
	return &HNSW{cfg: c, rng: rand.New(rand.NewPCG(c.Seed, c.Seed^0x9e3779b97f4a7c15)), nodes: map[string]*node{}, maxLevel: -1}, nil
}

// Validate checks whether a vector can be admitted without mutating the graph.
func (h *HNSW) Validate(v vector.Vector) error { return v.ValidateDimension(h.cfg.Dimension) }

// GetVector returns a copy of one indexed vector. It is intentionally a direct ID lookup,
// not an ANN query: API recovery paths need the exact durable payload rather than an
// approximate nearest neighbour.
func (h *HNSW) GetVector(id string) (vector.Vector, bool) {
	node, ok := h.nodes[id]
	if !ok {
		return nil, false
	}
	return append(vector.Vector(nil), node.v...), true
}
func (h *HNSW) level() int {
	u := h.rng.Float64()
	if u == 0 {
		u = math.SmallestNonzeroFloat64
	}
	return int(-math.Log(u) / math.Log(float64(h.cfg.M)))
}

// Insert adds or replaces an embedding. Equal inputs and insertion order produce equal graph
// mutations; callers must serialize insertions through Raft before invoking it on replicas.
func (h *HNSW) Insert(id string, v vector.Vector) error {
	if id == "" {
		return errors.New("ann: empty ID")
	}
	if err := v.ValidateDimension(h.cfg.Dimension); err != nil {
		return err
	}
	if _, ok := h.nodes[id]; ok {
		return errors.New("ann: duplicate ID")
	}
	n := &node{id: id, v: append(vector.Vector(nil), v...), level: h.level(), neighbors: map[int][]string{}}
	if len(h.nodes) == 0 {
		h.nodes[id] = n
		h.entry = id
		h.maxLevel = n.level
		return nil
	}
	// Publish before reciprocal-link trimming so distance comparisons can resolve this ID.
	h.nodes[id] = n
	for l := min(n.level, h.maxLevel); l >= 0; l-- {
		candidates := h.closestExcept(v, l, h.cfg.EFConstruction, id)
		selected := h.selectDiverse(v, candidates, h.cfg.M)
		for _, other := range selected {
			n.neighbors[l] = append(n.neighbors[l], other.ID)
			o := h.nodes[other.ID]
			o.neighbors[l] = append(o.neighbors[l], id)
			h.trim(o, l)
		}
	}
	if n.level > h.maxLevel {
		h.entry = id
		h.maxLevel = n.level
	}
	return nil
}
func (h *HNSW) trim(n *node, l int) {
	rs := make([]Result, 0, len(n.neighbors[l]))
	for _, id := range n.neighbors[l] {
		rs = append(rs, Result{ID: id, Distance: vector.L2Squared(n.v, h.nodes[id].v)})
	}
	sortResults(rs)
	if len(rs) > h.cfg.M {
		rs = rs[:h.cfg.M]
	}
	n.neighbors[l] = n.neighbors[l][:0]
	for _, r := range rs {
		n.neighbors[l] = append(n.neighbors[l], r.ID)
	}
}

// Search returns up to k approximate neighbours. EF controls the breadth/latency trade-off.
func (h *HNSW) Search(q vector.Vector, k, ef int) ([]Result, error) {
	if err := q.ValidateDimension(h.cfg.Dimension); err != nil {
		return nil, err
	}
	if k <= 0 {
		return nil, errors.New("ann: k must be positive")
	}
	if ef <= 0 {
		ef = h.cfg.EFSearch
	}
	if len(h.nodes) == 0 {
		return nil, nil
	}
	entry := h.entry
	for level := h.maxLevel; level > 0; level-- {
		candidates := h.searchLayer(q, entry, level, 1)
		if len(candidates) > 0 {
			entry = candidates[0].ID
		}
	}
	rs := h.searchLayer(q, entry, 0, ef)
	if len(rs) > k {
		rs = rs[:k]
	}
	return rs, nil
}

// searchLayer is HNSW's bounded best-first graph walk. The frontier is always expanded
// nearest-first; once its best candidate is worse than the current ef worst result, no
// remaining path can improve the bounded result set at this layer.
func (h *HNSW) searchLayer(q vector.Vector, entry string, level, ef int) []Result {
	start, ok := h.nodes[entry]
	if !ok {
		return nil
	}
	frontier := []Result{{ID: entry, Distance: vector.L2Squared(q, start.v)}}
	best := append([]Result(nil), frontier...)
	visited := map[string]bool{entry: true}
	for len(frontier) > 0 {
		sortResults(frontier)
		candidate := frontier[0]
		frontier = frontier[1:]
		sortResults(best)
		if len(best) >= ef && candidate.Distance > best[len(best)-1].Distance {
			break
		}
		for _, neighbor := range h.nodes[candidate.ID].neighbors[level] {
			if visited[neighbor] {
				continue
			}
			visited[neighbor] = true
			node := h.nodes[neighbor]
			if node == nil {
				continue
			}
			result := Result{ID: neighbor, Distance: vector.L2Squared(q, node.v)}
			if len(best) < ef || result.Distance < best[len(best)-1].Distance {
				frontier = append(frontier, result)
				best = append(best, result)
				sortResults(best)
				if len(best) > ef {
					best = best[:ef]
				}
			}
		}
	}
	sortResults(best)
	return best
}

// Repair removes nodes and cross-boundary edges rejected by keep, then re-trims affected
// neighbour lists. Range splitting calls this on each child graph so it never traverses an
// edge into a keyspace it no longer owns.
func (h *HNSW) Repair(keep func(id string) bool) {
	for id := range h.nodes {
		if !keep(id) {
			delete(h.nodes, id)
		}
	}
	for _, n := range h.nodes {
		for level, ids := range n.neighbors {
			filtered := ids[:0]
			for _, id := range ids {
				if _, ok := h.nodes[id]; ok {
					filtered = append(filtered, id)
				}
			}
			n.neighbors[level] = filtered
			h.trim(n, level)
		}
	}
	if _, ok := h.nodes[h.entry]; !ok {
		h.entry, h.maxLevel = "", -1
		for id, n := range h.nodes {
			if n.level > h.maxLevel {
				h.entry, h.maxLevel = id, n.level
			}
		}
	}
}

// Delete removes a vector and all reciprocal links to it. A replicated caller must log
// this mutation before applying it, just as it does insertion, so replicas remove the same
// graph topology in the same order.
func (h *HNSW) Delete(id string) error {
	if _, ok := h.nodes[id]; !ok {
		return errors.New("ann: ID not found")
	}
	h.Repair(func(candidate string) bool { return candidate != id })
	return nil
}
func (h *HNSW) closestExcept(q vector.Vector, l, limit int, exclude string) []Result {
	rs := make([]Result, 0)
	for id, n := range h.nodes {
		if id != exclude && n.level >= l {
			rs = append(rs, Result{ID: id, Distance: vector.L2Squared(q, n.v)})
		}
	}
	sortResults(rs)
	if len(rs) > limit {
		rs = rs[:limit]
	}
	return rs
}

// selectDiverse implements HNSW's diversity criterion: a candidate is accepted only when
// it is nearer to the new node than to already-selected neighbours, retaining long links.
func (h *HNSW) selectDiverse(q vector.Vector, candidates []Result, m int) []Result {
	out := make([]Result, 0, m)
	for _, c := range candidates {
		keep := true
		for _, s := range out {
			if vector.L2Squared(h.nodes[c.ID].v, h.nodes[s.ID].v) < c.Distance {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, c)
			if len(out) == m {
				break
			}
		}
	}
	return out
}
func sortResults(rs []Result) {
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].Distance == rs[j].Distance {
			return rs[i].ID < rs[j].ID
		}
		return rs[i].Distance < rs[j].Distance
	})
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
