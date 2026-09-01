// Command vectortorture drives real per-replica internal/ann.HNSW graphs, kept in sync
// through a real internal/raft.Cluster, under a fault schedule -- the vector-workload
// counterpart to cmd/torture, which does the same for a byte register.
//
// Before this existed, harness/torture/workload/vector.py checked a fixed, hardcoded
// three-element ID list, independent of the seed, the nemesis schedule, or the real Go
// HNSW implementation at all (see that file's docstring). ann.ReplicatedIndex -- the
// existing in-memory Raft+HNSW composition -- could not be reused directly: its commit()
// always uses Cluster.Propose, which always delivers every message, and it treats any
// replica missing a just-proposed entry as an error, which is exactly what a fault
// schedule needs to be able to do. This binary talks to internal/raft.Cluster and
// internal/ann.HNSW directly, the same layer cmd/torture already works at, instead of
// through that higher-level wrapper.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/ashraf/consensa/internal/ann"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/vector"
)

// fault mirrors harness/torture/nemesis.py's Fault dataclass, identical to cmd/torture's.
type fault struct {
	Step   int    `json:"step"`
	Kind   string `json:"kind"`
	Target int    `json:"target"`
}

// replicaReport is one node's final state: how many mutations it actually applied, and a
// canonical snapshot of the resulting graph, so the Python checker can compare replicas
// without understanding HNSW's internal structure at all.
type replicaReport struct {
	Node     int      `json:"node"`
	Applied  int      `json:"applied"`
	IDs      []string `json:"ids"`
	Snapshot string   `json:"snapshot"` // hex-independent: raw JSON bytes as a string, for byte-identity comparison
}

type report struct {
	Replicas []replicaReport `json:"replicas"`
}

func main() {
	nodeCount := flag.Int("nodes", 3, "cluster size")
	rounds := flag.Int("rounds", 30, "number of fault-eligible ticks to run")
	dimension := flag.Int("dimension", 4, "vector dimension")
	flag.Parse()

	var schedule []fault
	if err := json.NewDecoder(bufio.NewReader(os.Stdin)).Decode(&schedule); err != nil {
		fmt.Fprintf(os.Stderr, "vectortorture: reading fault schedule from stdin: %v\n", err)
		os.Exit(1)
	}
	byStep := map[int]fault{}
	for _, f := range schedule {
		byStep[f.Step] = f
	}

	ids := make([]raft.NodeID, *nodeCount)
	for i := range ids {
		ids[i] = raft.NodeID(i + 1)
	}
	cluster, err := raft.NewCluster(ids)
	if err != nil {
		fatal(err)
	}

	cfg := ann.Config{Dimension: *dimension, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1}
	replicas := map[raft.NodeID]*ann.HNSW{}
	appliedCount := map[raft.NodeID]int{}
	for _, id := range ids {
		index, err := ann.NewHNSW(cfg)
		if err != nil {
			fatal(err)
		}
		replicas[id] = index
	}

	// Elect an initial leader on a fully connected network before any fault fires --
	// same reasoning as cmd/torture: a fault targeting round 0 should test what happens
	// to an already-working cluster, not confound "never elected" with "a fault broke it".
	for i := 0; i < 3; i++ {
		if err := cluster.TickFiltered(alwaysDeliver); err != nil {
			fatal(err)
		}
	}

	// applyNewEntries brings replica id's local HNSW graph up to date with whatever
	// Cluster.Applied(id) has newly committed, mirroring how internal/ann.DurableNode's
	// apply() and ann.ReplicatedIndex.commit() both drive ApplyMutation from committed
	// Raft entries -- the difference here is this loop tolerates a replica that is behind
	// (isolated this round) instead of treating it as an error.
	applyNewEntries := func(id raft.NodeID) error {
		applied := cluster.Applied(id)
		for appliedCount[id] < len(applied) {
			if err := replicas[id].ApplyMutation(applied[appliedCount[id]]); err != nil {
				return fmt.Errorf("node %d: applying entry %d: %w", id, appliedCount[id], err)
			}
			appliedCount[id]++
		}
		return nil
	}

	for round := 0; round < *rounds; round++ {
		deliver := alwaysDeliver
		if f, ok := byStep[round]; ok {
			target := raft.NodeID(f.Target%(*nodeCount)) + 1
			deliver = isolate(target)
		}
		if err := cluster.TickFiltered(deliver); err != nil {
			fatal(err)
		}

		if leader, hasLeader := cluster.Leader(); hasLeader {
			vec := deterministicVector(round, *dimension)
			data, err := ann.EncodeMutation(fmt.Sprintf("v%d", round), vec)
			if err != nil {
				fatal(err)
			}
			// A rejected or non-committing propose is not an error at this layer, the
			// same way cmd/torture treats a proposal into an isolated leader: the driver
			// only cares what actually got committed, discovered below via Applied(),
			// not what Propose locally believed.
			_ = cluster.ProposeFiltered(leader, data, deliver)
		}

		for _, id := range ids {
			if err := applyNewEntries(id); err != nil {
				fatal(err)
			}
		}
	}

	var out report
	for _, id := range ids {
		snap, err := replicas[id].Snapshot()
		if err != nil {
			fatal(err)
		}
		out.Replicas = append(out.Replicas, replicaReport{
			Node:     int(id),
			Applied:  appliedCount[id],
			IDs:      idsIn(replicas[id]),
			Snapshot: string(snap),
		})
	}
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		fatal(err)
	}
}

// deterministicVector is a fixed function of round and dimension, not random, so two
// replicas that both apply "insert round N" always agree on the vector's content --
// necessary for the snapshot byte-identity check to mean anything.
func deterministicVector(round, dimension int) vector.Vector {
	v := make(vector.Vector, dimension)
	for i := range v {
		v[i] = float32(round + i)
	}
	return v
}

// idsIn extracts every node ID currently in the graph via a round trip through Snapshot,
// rather than adding a package-internal accessor to internal/ann just for this driver.
func idsIn(h *ann.HNSW) []string {
	snap, err := h.Snapshot()
	if err != nil {
		return nil
	}
	var decoded struct {
		Nodes []struct {
			ID string `json:"ID"`
		} `json:"Nodes"`
	}
	if err := json.Unmarshal(snap, &decoded); err != nil {
		return nil
	}
	out := make([]string, 0, len(decoded.Nodes))
	for _, n := range decoded.Nodes {
		out = append(out, n.ID)
	}
	return out
}

func alwaysDeliver(raft.Message) bool { return true }

func isolate(target raft.NodeID) func(raft.Message) bool {
	return func(m raft.Message) bool { return m.From != target && m.To != target }
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "vectortorture: %v\n", err)
	os.Exit(1)
}
