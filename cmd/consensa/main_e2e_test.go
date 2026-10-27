package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// This test runs three separate OS processes of the real consensa binary, talking over
// real TCP, each with its own data directory. It writes vectors, kills one process,
// keeps writing through the other two, restarts the killed process on its old directory,
// and checks that it serves the data again.
//
// It builds a binary and starts processes, so it is slower than the other tests and is
// skipped with -short.

// usedPorts remembers the ports freePort has handed out. Closing a listener frees the
// port, and the OS can give the same port to the very next request, so freePort retries
// if it sees a port it has already used.
var usedPorts = map[int]bool{}

func freePort(t *testing.T) int {
	t.Helper()
	for {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		if err := listener.Close(); err != nil {
			t.Fatalf("release port: %v", err)
		}
		if usedPorts[port] {
			continue
		}
		usedPorts[port] = true
		return port
	}
}

type e2eNode struct {
	id        int
	raftAddr  string
	grpcAddr  string
	dataDir   string
	binary    string
	peersFlag string
	cmd       *exec.Cmd
}

func (n *e2eNode) start(t *testing.T) {
	t.Helper()
	args := []string{
		"-id", fmt.Sprint(n.id),
		"-peers", n.peersFlag,
		"-data-dir", n.dataDir,
		"-grpc-listen", n.grpcAddr,
		"-dimension", "4",
		"-tick-interval", "20ms",
	}
	cmd := exec.Command(n.binary, args...)
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

// waitForListening waits until something accepts TCP connections on addr. It dials
// directly instead of using a gRPC connection, which would back off between retries.
// Start-up (opening the disk store, then the listeners) takes a variable amount of time.
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

func dialNode(t *testing.T, addr string) consensav1.ConsensaClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return consensav1.NewConsensaClient(conn)
}

// upsertUntilAccepted tries the insert on every node until one accepts it. Only the Raft
// leader accepts writes, and the client does not know which node that is.
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
			grpcAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			dataDir:  filepath.Join(t.TempDir(), fmt.Sprintf("node%d", id)),
			binary:   binary,
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

	// Kill node 3's real process -- a hard kill, not a graceful shutdown -- then confirm the
	// surviving two-process majority elects a new leader (if node 3 held it) and keeps
	// accepting writes over real gRPC.
	nodes[3].kill(t)
	upsertUntilAccepted(t, clients[:2], "d", []float32{0, 0, 0, 1}, 10*time.Second)

	// Restart node 3 as a new process on the same data directory, starting with an empty
	// graph. It then has to serve the earlier vectors again. Nodes 1 and 2 are still
	// running, so it could also catch up from them; ann.TestDurableNodeSurvivesRestart
	// is the test that proves recovery from the node's own disk alone.
	nodes[3] = &e2eNode{
		id: 3, raftAddr: nodes[3].raftAddr, grpcAddr: nodes[3].grpcAddr,
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

func TestParsePeers(t *testing.T) {
	got, err := parsePeers("1=a:1,2=b:2")
	if err != nil || len(got) != 2 || got[2] != "b:2" {
		t.Fatalf("parsePeers valid = %#v, %v", got, err)
	}
	for _, bad := range []string{"", "1", "0=a:1", "x=a:1", "1="} {
		if _, err := parsePeers(bad); err == nil {
			t.Fatalf("parsePeers(%q) accepted", bad)
		}
	}
}
