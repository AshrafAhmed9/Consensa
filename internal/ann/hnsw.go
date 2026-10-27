package ann

import (
	"errors"
	"math"
	"math/rand/v2"
	"sort"

	"github.com/ashraf/consensa/internal/vector"
)

// HNSW (Hierarchical Navigable Small World) is a graph for approximate nearest-neighbour
// search. Every vector is a node. Each node links to a few nearby nodes, and some nodes
// also exist on higher "levels" that have fewer nodes and longer jumps.
//
// To search, start at the top level and repeatedly move to whichever neighbour is closest
// to the query. Drop down a level when no neighbour is closer. On the bottom level,
// explore more widely to collect the best candidates. This checks far fewer vectors than
// comparing against all of them.
//
// This type is not safe for concurrent use, and it is deterministic: the same inserts in
// the same order with the same Seed always build the same graph. That is what lets every
// Raft replica end up with an identical graph.

// Config sets the shape of the graph.
//   - Dimension: length of every vector.
//   - M: how many neighbours each node keeps per level.
//   - EFConstruction: how many candidates are considered when inserting a vector.
//   - EFSearch: how many candidates are considered when searching (the default for Search).
//   - Seed: seeds the random choice of node levels.
type Config struct {
	Dimension, M, EFConstruction, EFSearch int
	Seed                                   uint64
}

// Result is one search hit. Distance is squared L2 distance, so smaller means closer.
type Result struct {
	ID       string
	Distance float32
}

// node is one vector in the graph.
type node struct {
	id        string
	v         vector.Vector
	level     int              // the highest level this node appears on
	neighbors map[int][]string // neighbour IDs on each level
}

// HNSW is the graph.
type HNSW struct {
	cfg      Config
	rng      *rand.Rand
	nodes    map[string]*node
	entry    string // node where every search starts (it has the highest level)
	maxLevel int    // the entry node's level, or -1 when the graph is empty
}

// NewHNSW creates an empty graph.
func NewHNSW(c Config) (*HNSW, error) {
	if c.Dimension <= 0 || c.M <= 0 || c.EFConstruction < c.M || c.EFSearch <= 0 {
		return nil, errors.New("ann: invalid HNSW config")
	}
	return &HNSW{cfg: c, rng: rand.New(rand.NewPCG(c.Seed, c.Seed^0x9e3779b97f4a7c15)), nodes: map[string]*node{}, maxLevel: -1}, nil
}

// Validate checks that a vector has the right length and no NaN/infinite values.
func (h *HNSW) Validate(v vector.Vector) error { return v.ValidateDimension(h.cfg.Dimension) }

// level picks a random level for a new node. Higher levels are exponentially rarer,
// so the upper levels stay small.
func (h *HNSW) level() int {
	u := h.rng.Float64()
	if u == 0 {
		u = math.SmallestNonzeroFloat64
	}
	return int(-math.Log(u) / math.Log(float64(h.cfg.M)))
}

// Insert adds a vector, replacing any existing vector with the same ID.
//
// A replace must never fail: Raft has already committed the insert, and a failing
// state machine would fail on it again after every restart.
//
// To place the new node, it finds its closest existing nodes on each level (by checking
// every node, which is exact but slow for large graphs), picks a diverse subset of
// them as neighbours, and links them both ways.
func (h *HNSW) Insert(id string, v vector.Vector) error {
	if id == "" {
		return errors.New("ann: empty ID")
	}
	if err := v.ValidateDimension(h.cfg.Dimension); err != nil {
		return err
	}
	if _, ok := h.nodes[id]; ok {
		if err := h.Delete(id); err != nil {
			return err
		}
	}
	n := &node{id: id, v: append(vector.Vector(nil), v...), level: h.level(), neighbors: map[int][]string{}}
	if len(h.nodes) == 0 {
		h.nodes[id] = n
		h.entry = id
		h.maxLevel = n.level
		return nil
	}
	// Add the node first: the distance code below looks nodes up by ID.
	h.nodes[id] = n
	for l := min(n.level, h.maxLevel); l >= 0; l-- {
		candidates := h.closest(v, l, h.cfg.EFConstruction, map[string]bool{id: true})
		for _, other := range h.selectDiverse(v, candidates, h.cfg.M) {
			h.link(n, h.nodes[other.ID], l)
		}
	}
	if n.level > h.maxLevel {
		h.entry = id
		h.maxLevel = n.level
	}
	return nil
}

// link connects a and b on level l in both directions. The other node's list is cut back
// to its M closest if it grew too long.
func (h *HNSW) link(a, b *node, l int) {
	a.neighbors[l] = append(a.neighbors[l], b.id)
	b.neighbors[l] = append(b.neighbors[l], a.id)
	h.trim(b, l)
}

// trim keeps only the M closest neighbours of n on level l.
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

// Search returns up to k approximate nearest neighbours of q, closest first. ef is how
// many candidates to track on the bottom level: bigger is slower but finds more of the
// true nearest neighbours. If ef is 0 the configured EFSearch is used.
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
	// Walk down from the top level, moving to the closest node found on each level.
	entry := h.entry
	for level := h.maxLevel; level > 0; level-- {
		candidates := h.searchLayer(q, entry, level, 1)
		if len(candidates) > 0 {
			entry = candidates[0].ID
		}
	}
	// On the bottom level, search more widely.
	rs := h.searchLayer(q, entry, 0, ef)
	if len(rs) > k {
		rs = rs[:k]
	}
	return rs, nil
}

// searchLayer walks one level of the graph starting at entry and returns the ef closest
// nodes it found, closest first. It always expands the closest unexplored candidate, and
// stops once that candidate is farther than the worst of the ef results, because
// anything reached through it could only be worse.
func (h *HNSW) searchLayer(q vector.Vector, entry string, level, ef int) []Result {
	start, ok := h.nodes[entry]
	if !ok {
		return nil
	}
	frontier := []Result{{ID: entry, Distance: vector.L2Squared(q, start.v)}} // still to explore
	best := append([]Result(nil), frontier...)                                // best found so far
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

// Delete removes a vector from the graph.
//
// Removing a node leaves its former neighbours with a missing link, so after dropping the
// links to it, every node that now has fewer than M neighbours on a level looks for new
// ones. Nodes are visited in sorted ID order and ties are broken by ID, so every replica
// repairs the graph in exactly the same way.
func (h *HNSW) Delete(id string) error {
	if _, ok := h.nodes[id]; !ok {
		return errors.New("ann: ID not found")
	}
	delete(h.nodes, id)
	for _, n := range h.nodes {
		for level, ids := range n.neighbors {
			kept := ids[:0]
			for _, neighborID := range ids {
				if neighborID != id {
					kept = append(kept, neighborID)
				}
			}
			n.neighbors[level] = kept
			h.trim(n, level)
		}
	}
	sortedIDs := h.sortedIDs()
	for _, nodeID := range sortedIDs {
		n := h.nodes[nodeID]
		for level := 0; level <= n.level; level++ {
			need := h.cfg.M - len(n.neighbors[level])
			if need <= 0 {
				continue
			}
			skip := map[string]bool{n.id: true}
			for _, existing := range n.neighbors[level] {
				skip[existing] = true
			}
			candidates := h.closest(n.v, level, h.cfg.EFConstruction, skip)
			for _, other := range h.selectDiverse(n.v, candidates, need) {
				h.link(n, h.nodes[other.ID], level)
			}
		}
	}
	if h.entry == id {
		// The entry node was deleted: pick the node with the highest level (lowest ID on ties).
		h.entry, h.maxLevel = "", -1
		for _, nodeID := range sortedIDs {
			if n := h.nodes[nodeID]; n.level > h.maxLevel {
				h.entry, h.maxLevel = nodeID, n.level
			}
		}
	}
	return nil
}

// sortedIDs returns every node ID in sorted order.
func (h *HNSW) sortedIDs() []string {
	ids := make([]string, 0, len(h.nodes))
	for id := range h.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// closest returns up to limit nodes closest to q that exist on level l or higher,
// leaving out the IDs in skip. It compares against every node.
func (h *HNSW) closest(q vector.Vector, l, limit int, skip map[string]bool) []Result {
	rs := make([]Result, 0)
	for id, n := range h.nodes {
		if !skip[id] && n.level >= l {
			rs = append(rs, Result{ID: id, Distance: vector.L2Squared(q, n.v)})
		}
	}
	sortResults(rs)
	if len(rs) > limit {
		rs = rs[:limit]
	}
	return rs
}

// selectDiverse picks up to m neighbours from candidates (closest first). A candidate is
// skipped if it is closer to an already-picked neighbour than to q. That spreads the
// links in different directions instead of clumping them, which keeps long jumps.
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

// sortResults orders results closest first, breaking ties by ID.
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
