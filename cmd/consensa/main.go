// Command consensa starts one replica of a real, disk-durable, TCP-networked vector store:
// a Raft host (internal/raft.Host) over real sockets, backed by a real storage.Engine, with
// its own HNSW graph (internal/ann.DurableNode) applying committed mutations. Every node in
// a deployment runs this same binary with the same --peers list and a different --id.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/ann"
	"github.com/ashraf/consensa/internal/metrics"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

func main() {
	id := flag.Uint64("id", 0, "this node's Raft ID (must be a key in --peers)")
	peersFlag := flag.String("peers", "", `Raft group members and their transport addresses, e.g. "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003" -- identical on every node in the deployment`)
	dataDir := flag.String("data-dir", "", "on-disk directory for this node's storage engine and Raft log (required, unique per node)")
	grpcListen := flag.String("grpc-listen", ":8080", "client-facing gRPC listen address")
	dimension := flag.Int("dimension", 3, "fixed collection vector dimension")
	metricsListen := flag.String("metrics-listen", ":9090", "Prometheus metrics listen address")
	tickInterval := flag.Duration("tick-interval", 50*time.Millisecond, "how often this node advances its Raft clock")
	flag.Parse()

	if *id == 0 {
		log.Fatal("consensa: --id is required and must be nonzero")
	}
	if *dataDir == "" {
		log.Fatal("consensa: --data-dir is required")
	}

	allPeers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("consensa: --peers: %v", err)
	}
	selfID := raft.NodeID(*id)
	selfAddr, ok := allPeers[selfID]
	if !ok {
		log.Fatalf("consensa: --id %d is not present in --peers", *id)
	}
	groupPeers := make([]raft.NodeID, 0, len(allPeers))
	transportPeers := make(map[raft.NodeID]string, len(allPeers)-1)
	for peerID, addr := range allPeers {
		groupPeers = append(groupPeers, peerID)
		if peerID != selfID {
			transportPeers[peerID] = addr
		}
	}
	sort.Slice(groupPeers, func(i, j int) bool { return groupPeers[i] < groupPeers[j] })

	node, err := ann.NewDurableNode(ann.DurableNodeConfig{
		ID: selfID, GroupPeers: groupPeers, ListenAddress: selfAddr, TransportPeers: transportPeers,
		StorageDir: *dataDir, Index: ann.Config{Dimension: *dimension, M: 16, EFConstruction: 64, EFSearch: 64, Seed: 1},
	})
	if err != nil {
		log.Fatalf("consensa: starting durable node: %v", err)
	}
	defer func() {
		if err := node.Close(); err != nil {
			log.Printf("consensa: closing node: %v", err)
		}
	}()

	metricRegistry := metrics.NewRegistry()

	// Raft only makes progress when something drives its clock; production wires that to
	// a real timer instead of the deterministic simulator's stepped scheduler tests use.
	stopTicking := make(chan struct{})
	go func() {
		ticker := time.NewTicker(*tickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopTicking:
				return
			case <-ticker.C:
				// A tick error here means "not leader" or a dial failure to a peer that
				// is temporarily down -- both are ordinary and handled by internal/raft's
				// own retry-on-next-heartbeat behavior, not something this loop must react to.
				_ = node.Tick()
				// Recall stays at its registered zero value here -- it has no real source
				// wired to this loop yet (it needs a benchmark hook from harness/bench).
				// Reporting it anyway would be exactly the kind of fabricated-looking
				// metric this project's own documentation standard argues against; leaving
				// it at zero is honest about what isn't measured, not a placeholder to
				// hide. RangeQPS is set separately below, since it's a rate over a window
				// rather than an instantaneous value like the Raft term.
				_, term, _ := node.Status()
				metricRegistry.RaftTerm.Set(float64(term))
			}
		}
	}()
	defer close(stopTicking)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(metricRegistry.Registry, promhttp.HandlerOpts{}))
		// consensa_ann_recall cannot be computed by this process itself: recall is only
		// meaningful relative to a labeled dataset and its brute-force ground truth,
		// neither of which this node has any reason to know about its own accord. This
		// endpoint accepts a value an external benchmark client already computed against
		// THIS node's real Search RPC (see harness/bench and
		// cmd/consensa/main_e2e_test.go's TestConsensaBinaryReportsRealRecallMetric for
		// the client side) -- the same "push" shape a Prometheus pushgateway uses, chosen
		// because this node cannot originate the measurement itself.
		mux.HandleFunc("/report-recall", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, 64))
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			value, err := strconv.ParseFloat(strings.TrimSpace(string(body)), 64)
			if err != nil || value < 0 || value > 1 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			metricRegistry.Recall.Set(value)
			w.WriteHeader(http.StatusNoContent)
		})
		if err := http.ListenAndServe(*metricsListen, mux); err != nil {
			log.Printf("consensa: metrics server stopped: %v", err)
		}
	}()

	// DurableNode satisfies server.Index directly: Insert/Delete only succeed when this
	// replica is the current Raft leader (see DurableNode.Insert's doc comment), so a
	// client that gets a "not leader" error is expected to retry against another node in
	// --peers -- this binary does not yet forward writes to the leader on a client's
	// behalf. Reads (Search/Validate) are served locally regardless of leadership.
	service := server.NewService(node)

	// consensa_range_qps is a rate, and Service.RequestCount is only a raw cumulative
	// counter (see its own doc comment for why), so this loop samples the delta over a
	// fixed window itself rather than pushing that responsibility onto Service.
	stopQPS := make(chan struct{})
	go func() {
		const window = time.Second
		ticker := time.NewTicker(window)
		defer ticker.Stop()
		var last uint64
		for {
			select {
			case <-stopQPS:
				return
			case <-ticker.C:
				current := service.RequestCount()
				metricRegistry.RangeQPS.Set(float64(current-last) / window.Seconds())
				last = current
			}
		}
	}()
	defer close(stopQPS)

	listener, err := net.Listen("tcp", *grpcListen)
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	consensav1.RegisterConsensaServer(grpcServer, service)
	log.Printf("consensa node %d: raft on %s, gRPC on %s, metrics on http://%s/metrics, data in %s",
		selfID, selfAddr, *grpcListen, fmt.Sprintf("%s", *metricsListen), *dataDir)
	log.Fatal(grpcServer.Serve(listener))
}

// parsePeers turns "1=host:port,2=host:port" into a NodeID -> address map. It is deliberately
// strict: a malformed entry fails the whole process at startup rather than silently running
// with a smaller cluster than the operator intended.
func parsePeers(raw string) (map[raft.NodeID]string, error) {
	if raw == "" {
		return nil, fmt.Errorf("must not be empty")
	}
	peers := map[raft.NodeID]string{}
	for _, entry := range strings.Split(raw, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("malformed entry %q, want id=host:port", entry)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("malformed node ID in %q: %v", entry, err)
		}
		peers[raft.NodeID(id)] = parts[1]
	}
	if len(peers) < 1 {
		return nil, fmt.Errorf("must name at least one node")
	}
	return peers, nil
}
