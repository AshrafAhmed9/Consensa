// Command torture drives a real internal/raft.Cluster under a fault schedule and prints a
// real client-observable operation history as JSON, so the Python torture harness's
// linearizability checker (harness/torture/checker/linearizability.py) can be run against
// something that actually happened, instead of a fixed hand-written history.
//
// Before this existed, harness/torture/workload/register.py checked a hardcoded two-
// operation history that never touched Go at all -- the seed and --nemesis flags in
// harness/torture/cli.py generated a fault schedule but nothing ever applied it to
// anything, so pass/fail was completely independent of both. This binary is what makes
// the seed and the nemesis schedule actually matter.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/ashraf/consensa/internal/raft"
)

// fault mirrors harness/torture/nemesis.py's Fault dataclass field-for-field, so the same
// JSON a Python-generated schedule produces decodes here without translation.
type fault struct {
	Step   int    `json:"step"`
	Kind   string `json:"kind"`
	Target int    `json:"target"`
}

// operation mirrors harness/torture/checker/linearizability.py's Operation dataclass.
type operation struct {
	Invocation int     `json:"invocation"`
	Response   int     `json:"response"`
	Kind       string  `json:"kind"`
	Value      *string `json:"value"`
	Result     *string `json:"result"`
}

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

	// Elect an initial leader on a fully connected network before any fault fires -- a
	// fault schedule targeting round 0 should test what happens to an ALREADY-WORKING
	// cluster, not confound "never elected a leader" with "a fault broke something."
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
			// "partition" and "crash" are modeled identically here -- full isolation of
			// the target node's messages for this one round -- which is a deliberate,
			// documented simplification (see docs/notes/06-torture.md), not an oversight:
			// distinguishing a genuine crash (node also stops ticking, loses no state vs.
			// keeps state) from a network partition (node keeps ticking, fully retains
			// state) needs a richer fault model than this first pass builds.
			deliver = isolate(target)
		}
		if err := cluster.TickFiltered(deliver); err != nil {
			fatal(err)
		}

		leader, hasLeader := cluster.Leader()
		if !hasLeader {
			continue
		}
		// A real client does not know whether the node it is talking to is currently
		// isolated; modeling that means proposing through "the leader" even when it might
		// be the fault's target this round, using the SAME filtered delivery so an
		// isolated leader's proposal genuinely cannot reach a quorum, exactly like a real
		// client's request would time out against a partitioned leader.
		value := fmt.Sprintf("v%d", round)
		invokeAt := logical
		respondAt := logical + 1
		proposeErr := cluster.ProposeFiltered(leader, []byte(value), deliver)
		logical += 2
		// Propose returning nil means "this node still believes it is leader and appended
		// the entry locally" -- NOT "a quorum committed it." An isolated leader keeps
		// believing it is leader (that is exactly the zombie-leader scenario Raft's
		// Election Safety property has to prevent) and Propose happily appends to its own
		// log regardless of whether any message actually left the node. A real client only
		// learns a write succeeded from a server-confirmed commit, so this driver checks
		// the leader's own Applied() tail rather than trusting a nil error -- the first
		// version of this tool trusted the nil error alone and recorded a write as
		// "succeeded" even when it was invisible to every other replica, which was a bug in
		// this test client's modeling, not a discovery about Consensa's Raft implementation.
		if proposeErr == nil {
			if applied := cluster.Applied(leader); len(applied) > 0 && string(applied[len(applied)-1]) == value {
				history = append(history, operation{Invocation: invokeAt, Response: respondAt, Kind: "write", Value: &value})
			}
			// else: proposed but not confirmed committed this round -- a real client's
			// request would still be in flight/timed out, so no operation is recorded,
			// matching the "a real client does not fabricate success" rule above.
		}

		// Read back through whatever this round's post-fault leader is (it may have
		// changed as a result of the fault). The read's invocation deliberately overlaps
		// the write's response instant rather than starting strictly after it, so the
		// checker has genuine concurrency to resolve instead of a trivially sequential
		// history -- an all-sequential history is linearizable by construction regardless
		// of correctness, which would make this check as vacuous as the fixed stub it
		// replaces.
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

func alwaysDeliver(raft.Message) bool { return true }

func isolate(target raft.NodeID) func(raft.Message) bool {
	return func(m raft.Message) bool { return m.From != target && m.To != target }
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "torture: %v\n", err)
	os.Exit(1)
}
