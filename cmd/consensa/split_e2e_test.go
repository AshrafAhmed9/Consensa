package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
)

// splitExecutedForParent polls every given node's /metrics until a
// consensa_kv_split_executed_total series for parentID appears with a nonzero value on
// AT LEAST ONE of them, or fails the test at deadline. Checking only one specific node
// would be wrong: which of the three processes actually completes the migration depends
// on which one's own local replica wins the new child group's leadership, which is not
// deterministically any particular node -- executeSplitIfRecommended's own doc comment
// explains why every process runs the identical check independently rather than one
// coordinator deciding. Unlike raftTermFromMetrics's exact-line match, this checks that
// the metric name and the parent_range_id label both appear on the same line, since the
// counter's full label set (left_range_id/right_range_id) is exactly what this test does
// NOT want to hardcode -- and cannot assume label ORDER either: prometheus's text
// exposition format sorts label names alphabetically, so "left_range_id" (l) always
// precedes "parent_range_id" (p) on the wire regardless of the order CounterVec's labels
// were declared in. A bare prefix match against `metricName{parent_range_id="..."`
// therefore never matches -- found as a real CI failure where the split logs proved
// migration succeeded on all three processes within a second, yet the test still timed
// out after 30s of scraping, because the line it was looking for could never appear.
func splitExecutedForParent(t *testing.T, metricAddrs []string, parentID int, deadline time.Duration) {
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
	t.Fatalf("no %s{...%s...} series appeared on any of %v within %s -- live split never executed", name, label, metricAddrs, deadline)
}

func metricLineExists(t *testing.T, metricAddr, name, label string) bool {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", metricAddr))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, name+"{") && strings.Contains(line, label) {
			return true
		}
	}
	return false
}

// TestConsensaBinaryExecutesALiveSplitAutomatically proves the full runtime split-
// execution wiring inside the real shipped binary, not just the library primitive
// (internal/kv/execute_split_test.go) or the decision alone
// (TestCheckSplitRecommendationsSetsGaugeAboveThreshold). Three real processes are
// started with a --split-threshold low enough to cross with a handful of real
// TransactionalPut writes; once the parent range's key count exceeds it, this test
// confirms consensa_kv_split_executed_total actually appears (proving migration
// completed, not just that the decision gauge flipped), and that new writes spanning
// both halves of the split keyspace keep succeeding afterward -- proving
// Meta.Replace/KVService.RegisterStore/AdminService.RegisterRange correctly cut real
// traffic over to the fresh child ranges, not just replicated data into orphaned ones
// nothing can route to.
func TestConsensaBinaryExecutesALiveSplitAutomatically(t *testing.T) {
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
			// write tens of thousands of keys or wait the default 5s cadence to observe
			// a real split within a reasonable test budget.
			extraArgs: []string{"-split-threshold", "3", "-split-check-interval", "200ms"},
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

	var kvClients []consensav1.ConsensaKVClient
	for _, id := range ids {
		waitForListening(t, nodes[id].grpcAddr, 10*time.Second)
		kvClients = append(kvClients, dialKVNode(t, nodes[id].grpcAddr))
	}

	// The default --kv-split-key is "m", so keys below it belong to range 1 (left).
	// Four distinct keys cross the threshold of 3.
	for i, key := range []string{"a1", "a2", "a3", "a4"} {
		transactionalPutUntilAccepted(t, kvClients, fmt.Sprintf("seed-%d", i), map[string][]byte{key: []byte("v")}, 40*time.Second)
	}

	var metricAddrs []string
	for _, id := range ids {
		metricAddrs = append(metricAddrs, nodes[id].metricAddr)
	}
	splitExecutedForParent(t, metricAddrs, 1, 30*time.Second)

	// New writes spanning both sides of the original "m" boundary must keep succeeding
	// now that range 1 has been replaced by two fresh children in every process's
	// routing metadata -- if RegisterStore/RegisterRange had not actually wired the new
	// children into the running KVService/AdminService, this would now fail with "no
	// participant configured for range" instead of the old range simply not existing.
	transactionalPutUntilAccepted(t, kvClients, "after-split-left", map[string][]byte{"a5": []byte("v")}, 20*time.Second)
	transactionalPutUntilAccepted(t, kvClients, "after-split-right", map[string][]byte{"z1": []byte("v")}, 20*time.Second)
}

// binPath builds the real consensa binary once for this test file, mirroring
// TestConsensaBinaryThreeProcessClusterSurvivesKillAndRestart's own build step.
func binPath(t *testing.T, dir string) string {
	t.Helper()
	binary := dir + "/consensa"
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Dir = "."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building consensa binary: %v\n%s", err, out)
	}
	return binary
}
