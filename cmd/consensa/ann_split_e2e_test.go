package main

import (
	"fmt"
	"testing"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
)

// TestConsensaBinaryExecutesALiveVectorSplitAutomatically is
// TestConsensaBinaryExecutesALiveSplitAutomatically's (split_e2e_test.go) vector-plane
// counterpart: it proves executeAnnSplitIfRecommended's full runtime wiring inside the
// real shipped binary, not just ann.ExecuteLiveSplit as a library primitive
// (internal/ann/execute_split_test.go). Three real processes are started with a
// --split-threshold low enough to cross with a handful of real Upsert writes; once the
// vector range's applied vector count exceeds it, this test confirms
// consensa_kv_split_executed_total actually appears for the vector plane's own parent/
// child range IDs (1/101/102, distinct from the KV plane's 1/11/12 -- see
// executeAnnSplitIfRecommended's own doc comment for why *100 was chosen), then proves
// new writes spanning both sides of the split still succeed and are searchable --
// confirming server.Service.RegisterIndex/ann.Meta.Replace actually cut real traffic over
// to the fresh children, not just replicated data into orphaned ranges nothing can route to.
func TestConsensaBinaryExecutesALiveVectorSplitAutomatically(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs three real OS processes; skipped in -short mode")
	}

	binDir := t.TempDir()
	binary := binPath(t, binDir)

	ids := []int{1, 2, 3}
	nodes := map[int]*e2eNode{}
	peerParts := ""
	for i, id := range ids {
		if i > 0 {
			peerParts += ","
		}
		raftAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
		peerParts += fmt.Sprintf("%d=%s", id, raftAddr)
		nodes[id] = &e2eNode{
			id: id, raftAddr: raftAddr,
			grpcAddr:   fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			metricAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			dataDir:    t.TempDir(),
			binary:     binary,
			// A low threshold and a fast check interval so this test doesn't need to
			// upsert tens of thousands of vectors or wait the default 5s cadence to
			// observe a real split within a reasonable test budget.
			extraArgs: []string{"-split-threshold", "3", "-split-check-interval", "200ms", "-merge-threshold", "5", "-merge-qps-threshold", "100"},
		}
	}
	for _, id := range ids {
		nodes[id].peersFlag = peerParts
		nodes[id].start(t)
	}
	defer func() {
		for _, id := range ids {
			nodes[id].kill(t)
		}
	}()

	var clients []consensav1.ConsensaClient
	for _, id := range ids {
		waitForListening(t, nodes[id].grpcAddr, 10*time.Second)
		clients = append(clients, dialNode(t, nodes[id].grpcAddr))
	}

	// ann's split boundary is the lexicographic median ID (ann.ShouldSplit's own doc
	// comment), not a caller-chosen key like the KV plane's --kv-split-key -- five
	// distinct IDs cross the threshold of 3 regardless of where the median lands.
	for i, id := range []string{"a1", "a2", "a3", "a4", "a5"} {
		upsertUntilAccepted(t, clients, id, []float32{float32(i), 0, 0, 0}, 40*time.Second)
	}

	var metricAddrs []string
	for _, id := range ids {
		metricAddrs = append(metricAddrs, nodes[id].metricAddr)
	}
	annSplitExecutedForParent(t, metricAddrs, 1, 40*time.Second)
	// A measured quiet window after the split must drive the full vector merge path before
	// subsequent upserts prove the surviving left graph remains the live destination.
	mergeExecutedForParent(t, metricAddrs, 1, 60*time.Second)

	// New writes must keep succeeding, and be searchable, now that the vector plane's
	// range 1 has been replaced by two fresh children (101/102) in every process's
	// routing metadata -- if RegisterIndex/meta.Replace had not actually wired the new
	// children into the running Service, this would fail outright (no route) instead of
	// succeeding via the fan-out Search added for exactly this multi-range case.
	upsertUntilAccepted(t, clients, "after-split-1", []float32{9, 0, 0, 0}, 20*time.Second)
	upsertUntilAccepted(t, clients, "after-split-2", []float32{-9, 0, 0, 0}, 20*time.Second)
}

// annSplitExecutedForParent mirrors splitExecutedForParent (split_e2e_test.go) exactly,
// for the identical reason: which of the three processes actually completes the vector
// migration depends on which one's own local replica wins each fresh child group's
// leadership, not deterministically any particular node.
func annSplitExecutedForParent(t *testing.T, metricAddrs []string, parentID int, deadline time.Duration) {
	t.Helper()
	name := "consensa_kv_split_executed_total"
	label := fmt.Sprintf(`parent_range_id="%d"`, parentID)
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		for _, addr := range metricAddrs {
			if metricLineExists(t, addr, name, label) {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no %s{...%s...} series appeared on any of %v within %s -- live vector split never executed", name, label, metricAddrs, deadline)
}
