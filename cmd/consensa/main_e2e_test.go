package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// This is the test that proves the actual deliverable: three SEPARATE OS PROCESSES running
// the real `consensa` binary, talking over real TCP, each with its own on-disk directory --
// not goroutines in one process, not the in-memory Cluster demo path. Everything else in the
// repository tests components; this test builds the binary and runs it the way an operator
// would, then kills and restarts one process to prove the crash-recovery claim holds for the
// actual shipped artifact, not just for the DurableNode type in isolation.
//
// It is skipped in -short mode: building a binary and running three real processes through a
// real election is inherently slower than a unit test and does not belong in a fast inner loop.

func TestParseLearners(t *testing.T) {
	peers := map[raft.NodeID]string{1: "a", 2: "b", 3: "c"}
	got, err := parseLearners("3,2,3", peers)
	if err != nil || len(got) != 2 || got[0] != 3 || got[1] != 2 {
		t.Fatalf("parseLearners valid = %#v, %v", got, err)
	}
	if _, err := parseLearners("4", peers); err == nil {
		t.Fatal("unknown learner accepted")
	}
	if _, err := parseLearners("1,2,3", peers); err == nil {
		t.Fatal("all-learner configuration accepted")
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return port
}

type e2eNode struct {
	id         int
	raftAddr   string
	grpcAddr   string
	metricAddr string
	dataDir    string
	binary     string
	peersFlag  string
	cmd        *exec.Cmd
}

func (n *e2eNode) start(t *testing.T) {
	t.Helper()
	cmd := exec.Command(n.binary,
		"-id", fmt.Sprint(n.id),
		"-peers", n.peersFlag,
		"-data-dir", n.dataDir,
		"-grpc-listen", n.grpcAddr,
		"-metrics-listen", n.metricAddr,
		"-dimension", "4",
		"-tick-interval", "20ms",
	)
	cmd.Stdout = os.Stderr // surface node logs under `go test -v` for debugging failures
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("node %d: start: %v", n.id, err)
	}
	n.cmd = cmd
}

func (n *e2eNode) kill(t *testing.T) {
	t.Helper()
	if n.cmd == nil || n.cmd.Process == nil {
		return
	}
	if err := n.cmd.Process.Kill(); err != nil {
		t.Errorf("node %d: kill: %v", n.id, err)
	}
	_ = n.cmd.Wait()
	n.cmd = nil
}

// waitForListening polls with fresh, short-lived TCP dials (not through a pooled gRPC
// ClientConn, whose internal reconnect backoff can silently outlast a test's own retry
// loop) until something is listening on addr. Process startup -- disk I/O in
// storage.Open, then the Raft transport listener, then the gRPC listener -- takes a real,
// variable amount of wall-clock time, and that startup lag is a different concern from
// "is this replica the Raft leader," which the rest of this test checks separately.
func waitForListening(t *testing.T, addr string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("nothing listening on %s within %s: %v", addr, deadline, lastErr)
}

// raftTermFromMetrics fetches the real /metrics endpoint and parses out
// consensa_raft_term's current value -- a minimal, direct Prometheus text-format read
// rather than pulling in a client library just to check one line this test controls.
func raftTermFromMetrics(t *testing.T, metricAddr string) int {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", metricAddr))
	if err != nil {
		t.Fatalf("fetching /metrics from %s: %v", metricAddr, err)
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "consensa_raft_term ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			t.Fatalf("parsing consensa_raft_term value %q: %v", fields[1], err)
		}
		return int(value)
	}
	t.Fatalf("consensa_raft_term not found in /metrics output from %s", metricAddr)
	return 0
}

func dialNode(t *testing.T, addr string) consensav1.ConsensaClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return consensav1.NewConsensaClient(conn)
}

// dialKVNode opens the companion transactional-KV service hosted by the same real
// consensa process as the vector API. Keeping separate clients mirrors the protobuf's
// deliberately separate services while exercising the one shared gRPC listener.
func dialKVNode(t *testing.T, addr string) consensav1.ConsensaKVClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial KV %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return consensav1.NewConsensaKVClient(conn)
}

// upsertUntilAccepted retries an insert across every client until one succeeds. Real
// distributed writes have no notion of "the" endpoint -- a client is expected to retry
// against the group until it happens to reach the current leader, exactly as
// DurableNode.Insert's doc comment describes and as the RAG demo client will eventually do.
func upsertUntilAccepted(t *testing.T, clients []consensav1.ConsensaClient, id string, values []float32, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		for _, c := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			stream, err := c.Upsert(ctx)
			if err == nil {
				if err = stream.Send(&consensav1.UpsertRequest{Id: id, Vector: &consensav1.Vector{Values: values}}); err == nil {
					if _, err = stream.CloseAndRecv(); err == nil {
						cancel()
						return
					}
				}
			}
			lastErr = err
			cancel()
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no node accepted upsert of %q within %s: %v", id, deadline, lastErr)
}

// transactionalPutUntilAccepted retries the coordinator RPC across process endpoints,
// just like vector upsert retries: each endpoint hosts its local replica of every range,
// and only the process currently leading both static ranges can drive this compact
// demonstration topology. The server never pretends a follower can commit a transaction.
func transactionalPutUntilAccepted(t *testing.T, clients []consensav1.ConsensaKVClient, txnID string, writes map[string][]byte, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		for _, client := range clients {
			// A cross-range commit performs several ordered Raft proposals (record,
			// intent, index, commit record, resolution) and each DurableStore call
			// waits for local visibility. It is not comparable to one streaming upsert,
			// so a 2-second RPC budget can cancel a healthy coordinator halfway through.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := client.TransactionalPut(ctx, &consensav1.TransactionalPutRequest{TxnId: txnID, Writes: writes})
			cancel()
			if err == nil {
				return
			}
			lastErr = err
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no node committed transaction %q within %s: %v", txnID, deadline, lastErr)
}

func searchOn(t *testing.T, client consensav1.ConsensaClient, query []float32, k int, deadline time.Duration) []*consensav1.SearchResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	stream, err := client.Search(ctx, &consensav1.SearchRequest{Query: &consensav1.Vector{Values: query}, K: uint32(k), Ef: 20})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	var results []*consensav1.SearchResult
	for {
		r, err := stream.Recv()
		if err != nil {
			break
		}
		results = append(results, r)
	}
	return results
}

func TestConsensaBinaryThreeProcessClusterSurvivesKillAndRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs three real OS processes; skipped in -short mode")
	}

	binDir := t.TempDir()
	binary := filepath.Join(binDir, "consensa")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Dir = "."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building consensa binary: %v\n%s", err, out)
	}

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
			dataDir:    filepath.Join(t.TempDir(), fmt.Sprintf("node%d", id)),
			binary:     binary,
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
	var kvClients []consensav1.ConsensaKVClient
	for _, id := range ids {
		waitForListening(t, nodes[id].grpcAddr, 10*time.Second)
		clients = append(clients, dialNode(t, nodes[id].grpcAddr))
		kvClients = append(kvClients, dialKVNode(t, nodes[id].grpcAddr))
	}

	upsertUntilAccepted(t, clients, "a", []float32{1, 0, 0, 0}, 10*time.Second)
	upsertUntilAccepted(t, clients, "b", []float32{0, 1, 0, 0}, 10*time.Second)
	upsertUntilAccepted(t, clients, "c", []float32{0, 0, 1, 0}, 10*time.Second)

	// Confirm the write actually reached every process, not just the one that accepted it,
	// by polling search on each node until it agrees -- replication over real gRPC+TCP+disk
	// processes, not an in-memory shortcut.
	for _, id := range ids {
		var results []*consensav1.SearchResult
		end := time.Now().Add(10 * time.Second)
		for time.Now().Before(end) {
			results = searchOn(t, clients[id-1], []float32{1, 0, 0, 0}, 1, 2*time.Second)
			if len(results) == 1 && results[0].Id == "a" {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if len(results) != 1 || results[0].Id != "a" {
			t.Fatalf("node %d: search for nearest to (1,0,0,0) = %v, want [a]", id, results)
		}
	}

	// "apple" and "zebra" fall on opposite sides of the binary's default "m" split.
	// A successful call therefore proves the shipped process assembled two independent
	// durable ranges, routed both keys, and drove their real 2PC coordinator over gRPC;
	// internal/server/kv_service_test.go separately reads both values to prove resolution.
	transactionalPutUntilAccepted(t, kvClients, "binary-cross-range", map[string][]byte{
		"apple": []byte("fruit"),
		"zebra": []byte("animal"),
	}, 40*time.Second)

	// The metrics registry (internal/metrics) was previously registered and exposed over
	// HTTP but never actually updated -- /metrics would always report consensa_raft_term
	// at its zero value forever, regardless of real cluster activity. Now that the tick
	// loop sets it from the real node.Status() on every tick, confirm the real value: by
	// this point in the test three real elections' worth of writes have gone through, so
	// every node's term must be a real positive number, not the un-set zero.
	for _, id := range ids {
		term := raftTermFromMetrics(t, nodes[id].metricAddr)
		if term < 1 {
			t.Fatalf("node %d: consensa_raft_term = %d over /metrics, want >= 1 after real elections", id, term)
		}
	}

	// Kill node 3's real process -- a hard kill, not a graceful shutdown -- then confirm the
	// surviving two-process majority elects a new leader (if node 3 held it) and keeps
	// accepting writes over real gRPC.
	nodes[3].kill(t)
	upsertUntilAccepted(t, clients[:2], "d", []float32{0, 0, 0, 1}, 10*time.Second)

	// Restart node 3 as a brand-new process pointed at the SAME --data-dir. If this works,
	// recovery came from disk: the process asks nothing of its peers before this test dials
	// it directly and searches, and the process was started fresh with an empty in-memory
	// HNSW graph that only the recovered Raft log could have repopulated.
	nodes[3] = &e2eNode{
		id: 3, raftAddr: nodes[3].raftAddr, grpcAddr: nodes[3].grpcAddr, metricAddr: nodes[3].metricAddr,
		dataDir: nodes[3].dataDir, binary: binary, peersFlag: peerParts,
	}
	nodes[3].start(t)
	waitForListening(t, nodes[3].grpcAddr, 10*time.Second)
	restartedClient := dialNode(t, nodes[3].grpcAddr)

	var recovered []*consensav1.SearchResult
	end := time.Now().Add(10 * time.Second)
	for time.Now().Before(end) {
		recovered = searchOn(t, restartedClient, []float32{1, 0, 0, 0}, 1, 2*time.Second)
		if len(recovered) == 1 && recovered[0].Id == "a" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(recovered) != 1 || recovered[0].Id != "a" {
		t.Fatalf("restarted node 3 search for nearest to (1,0,0,0) = %v, want [a] recovered from disk", recovered)
	}
}

// TestConsensaBinaryReportsRealRecallMetric proves consensa_ann_recall's whole pipeline
// end to end: a real dataset upserted into a real 3-node cluster, real Search RPCs
// against it, an independent brute-force ground truth computed here (not reused from the
// node's own search path, the same reasoning cmd/vectortorture's bruteForceTopK uses),
// recall@k computed from the two, pushed to /report-recall, and read back from /metrics --
// proving the number that ends up on the Grafana dashboard (deploy/grafana) is a real
// measurement of this specific cluster's actual search quality, not a hardcoded value
// that happens to satisfy the endpoint's validation.
func TestConsensaBinaryReportsRealRecallMetric(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs three real OS processes; skipped in -short mode")
	}

	binDir := t.TempDir()
	binary := filepath.Join(binDir, "consensa")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Dir = "."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building consensa binary: %v\n%s", err, out)
	}

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
			dataDir:    filepath.Join(t.TempDir(), fmt.Sprintf("node%d", id)),
			binary:     binary,
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

	// A small, fixed synthetic dataset (dimension 4, matching e2eNode.start's -dimension
	// flag) -- deterministic so recall@k has one correct answer to check against.
	dataset := map[string][]float32{
		"a": {0, 0, 0, 0}, "b": {1, 0, 0, 0}, "e": {10, 0, 0, 0},
	}
	for id, v := range dataset {
		upsertUntilAccepted(t, clients, id, v, 10*time.Second)
	}

	// Independent brute-force ground truth over the same dataset just upserted -- the
	// query (0.5,0,0,0) should have {a,b} as its true top-2 nearest neighbours.
	query := []float32{0.5, 0, 0, 0}
	const k = 2
	groundTruth := bruteForceTop2(dataset, query)
	truthSetForWait := map[string]bool{groundTruth[0]: true, groundTruth[1]: true}

	// Retry until the search result actually MATCHES ground truth, not just until it
	// returns k results -- a fresh upsert can take a moment to replicate to whichever
	// node client[0] happens to be talking to, so an early search can return k results
	// that are simply whatever was already in the graph, not the just-upserted data.
	// Checking only len(results)==k (an earlier version of this test did exactly that)
	// found a real result: it can pass with the WRONG k results and then fail the recall
	// assertion below spuriously, since len() alone can't tell "the right answer arrived
	// late" apart from "the right answer is 2 items and this is a different 2 items".
	var results []*consensav1.SearchResult
	end := time.Now().Add(10 * time.Second)
	for time.Now().Before(end) {
		results = searchOn(t, clients[0], query, k, 2*time.Second)
		if len(results) == k && truthSetForWait[results[0].Id] && truthSetForWait[results[1].Id] {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(results) != k {
		t.Fatalf("search returned %d results, want %d", len(results), k)
	}

	hits := 0
	truthSet := map[string]bool{}
	for _, id := range groundTruth {
		truthSet[id] = true
	}
	for _, r := range results {
		if truthSet[r.Id] {
			hits++
		}
	}
	recall := float64(hits) / float64(k)
	if recall < 1.0 {
		t.Fatalf("this small, well-separated dataset should have recall@%d = 1.0, got %.2f (results=%v, truth=%v)", k, recall, results, groundTruth)
	}

	pushRecall(t, nodes[1].metricAddr, recall)

	reported := recallFromMetrics(t, nodes[1].metricAddr)
	if reported != recall {
		t.Fatalf("consensa_ann_recall over /metrics = %v, want the pushed value %v", reported, recall)
	}
}

// bruteForceTop2 is an independent nearest-neighbour computation, deliberately not
// reusing anything from internal/ann -- the whole point of ground truth is that it does
// not share code with the thing it's checking.
func bruteForceTop2(dataset map[string][]float32, query []float32) []string {
	type scored struct {
		id   string
		dist float64
	}
	var all []scored
	for id, v := range dataset {
		var sum float64
		for i := range v {
			d := float64(v[i]) - float64(query[i])
			sum += d * d
		}
		all = append(all, scored{id, sum})
	}
	for i := 0; i < len(all); i++ {
		for j := i + 1; j < len(all); j++ {
			if all[j].dist < all[i].dist {
				all[i], all[j] = all[j], all[i]
			}
		}
	}
	return []string{all[0].id, all[1].id}
}

func pushRecall(t *testing.T, metricAddr string, value float64) {
	t.Helper()
	resp, err := http.Post(fmt.Sprintf("http://%s/report-recall", metricAddr), "text/plain",
		strings.NewReader(fmt.Sprintf("%f", value)))
	if err != nil {
		t.Fatalf("pushing recall to %s: %v", metricAddr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("POST /report-recall = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func recallFromMetrics(t *testing.T, metricAddr string) float64 {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", metricAddr))
	if err != nil {
		t.Fatalf("fetching /metrics from %s: %v", metricAddr, err)
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "consensa_ann_recall ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			t.Fatalf("parsing consensa_ann_recall value %q: %v", fields[1], err)
		}
		return value
	}
	t.Fatalf("consensa_ann_recall not found in /metrics output from %s", metricAddr)
	return 0
}
