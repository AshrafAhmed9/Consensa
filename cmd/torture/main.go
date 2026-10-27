// Command torture runs a small Raft cluster (internal/raft.Cluster, all in one process)
// under a schedule of faults and prints the reads and writes a client would have seen, as
// JSON. The Python harness (harness/torture) reads that and checks the history is
// linearizable, meaning it could have come from a single correct copy of the data.
//
// The fault schedule comes in on stdin as JSON, produced from a seed by
// harness/torture/nemesis.py, so the same seed always runs the same faults.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/ashraf/consensa/internal/raft"
)

// fault is one entry of the schedule: at Step, cut Target off from the network. It matches nemesis.py.
type fault struct {
	Step   int    `json:"step"`
	Kind   string `json:"kind"`
	Target int    `json:"target"`
}

// operation is one read or write a client saw. It matches Operation in linearizability.py.
type operation struct {
	Invocation int     `json:"invocation"`
	Response   int     `json:"response"`
	Kind       string  `json:"kind"`
	Value      *string `json:"value"`
	Result     *string `json:"result"`
}

// report is the JSON printed at the end.
type report struct {
	History []operation `json:"history"`
}

func main() {
	nodeCount := flag.Int("nodes", 3, "cluster size")
	rounds := flag.Int("rounds", 30, "number of fault-eligible ticks to run")
	flag.Parse()

	var schedule []fault
	if err := json.NewDecoder(bufio.NewReader(os.Stdin)).Decode(&schedule); err != nil {
		fmt.Fprintf(os.Stderr, "torture: reading fault schedule from stdin: %v\n", err)
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

	// Let the cluster elect a leader on a healthy network first, so faults hit a working
	// cluster rather than one that never got started.
	for i := 0; i < 3; i++ {
		if err := cluster.TickFiltered(alwaysDeliver); err != nil {
			fatal(err)
		}
	}

	var history []operation
	logical := 0
	for round := 0; round < *rounds; round++ {
		deliver := alwaysDeliver
		if f, ok := byStep[round]; ok {
			target := raft.NodeID(f.Target%(*nodeCount)) + 1
			// "partition" and "crash" are treated the same: the target node is cut off from
			// every other node for this round. A real crash would also stop the node's
			// clock, and that is not modeled here.
			deliver = isolate(target)
		}
		if err := cluster.TickFiltered(deliver); err != nil {
			fatal(err)
		}

		leader, hasLeader := cluster.Leader()
		if !hasLeader {
			continue
		}
		// Send the write through the leader even if the fault just cut it off, as a real
		// client would. An isolated leader cannot reach a quorum, so its write must not commit.
		value := fmt.Sprintf("v%d", round)
		invokeAt := logical
		respondAt := logical + 1
		proposeErr := cluster.ProposeFiltered(leader, []byte(value), deliver)
		logical += 2
		// A nil error only means the leader added the entry to its own log. It does not
		// mean a quorum committed it (an isolated leader still accepts proposals). So the
		// write is only recorded if it shows up as applied on the leader.
		if proposeErr == nil {
			if applied := cluster.Applied(leader); len(applied) > 0 && string(applied[len(applied)-1]) == value {
				history = append(history, operation{Invocation: invokeAt, Response: respondAt, Kind: "write", Value: &value})
			}
			// Otherwise the write did not commit this round, so nothing is recorded.
		}

		// Read the latest applied value from whoever is leader now. The read starts at the
		// instant the write finished, so the two overlap and the checker has real
		// ordering questions to answer (a strictly sequential history is always linearizable).
		if readLeader, ok := cluster.Leader(); ok {
			applied := cluster.Applied(readLeader)
			var result *string
			if len(applied) > 0 {
				r := string(applied[len(applied)-1])
				result = &r
			}
			readInvoke := respondAt
			readRespond := logical
			history = append(history, operation{Invocation: readInvoke, Response: readRespond, Kind: "read", Result: result})
			logical++
		}
	}

	if err := json.NewEncoder(os.Stdout).Encode(report{History: history}); err != nil {
		fatal(err)
	}
}

// alwaysDeliver lets every message through.
func alwaysDeliver(raft.Message) bool { return true }

// isolate drops every message to or from target.
func isolate(target raft.NodeID) func(raft.Message) bool {
	return func(m raft.Message) bool { return m.From != target && m.To != target }
}

// fatal prints the error and exits.
func fatal(err error) {
	fmt.Fprintf(os.Stderr, "torture: %v\n", err)
	os.Exit(1)
}
