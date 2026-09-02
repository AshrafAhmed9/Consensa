// Command consensa starts one replica of a real, disk-durable, TCP-networked vector store:
// a Raft host (internal/raft.Host) over real sockets, backed by a real storage.Engine, with
// its own HNSW graph (internal/ann.DurableNode) applying committed mutations. Every node in
// a deployment runs this same binary with the same --peers list and a different --id.
package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/ann"
	"github.com/ashraf/consensa/internal/kv"
	"github.com/ashraf/consensa/internal/metrics"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/server"
	"github.com/ashraf/consensa/internal/txn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

// closedTimestampRange is the subset of kv.DurableRange advanceClosedTimestamps needs --
// declared narrowly, matching internal/txn's own rangeClient pattern, so a test can drive
// this exact logic against real kv.DurableRange leaders without pulling in the rest of
// main's process wiring (transport, gRPC, metrics).
type closedTimestampRange interface {
	AdvanceClosedTimestamp(ts time.Time) error
}

// advanceClosedTimestamps proposes closedAt (now, minus a safety lag) as the new closed
// timestamp on every range passed in. AdvanceClosedTimestamp is a no-op error on whichever
// replica isn't currently leader -- exactly like Propose -- so calling this against every
// replica in a deployment, not just "the leader," is correct and self-correcting across
// leadership changes without this function needing to track who's leader itself.
func advanceClosedTimestamps(now time.Time, lag time.Duration, ranges ...closedTimestampRange) {
	closedAt := now.Add(-lag)
	for _, r := range ranges {
		_ = r.AdvanceClosedTimestamp(closedAt)
	}
}

// leaseRange is the subset of kv.DurableRange maintainLeases needs.
type leaseRange interface {
	Status() (raft.Role, uint64)
	CurrentLease() kv.Lease
	GrantLease(holder raft.NodeID, duration time.Duration) error
}

// maintainLeases closes the gap docs/notes/09-leases.md and the README named as still
// missing: closed timestamps already advance on a real interval (advanceClosedTimestamps
// above), but nothing granted the lease that FollowerRead requires in the first place. The
// policy is deliberately the simplest one that is still self-correcting: whichever replica
// is currently Raft leader grants itself a fresh lease once its current one is not valid
// comfortably past renewBefore -- so a lease is renewed well before it actually expires
// (avoiding a gap where no valid lease exists at all) without this function tracking any
// state of its own between calls. A former leader that lost the role simply stops being
// called with role == Leader on the next check; it never proposes a lease it can't commit,
// since GrantLease (like Put) already no-ops non-leader proposals.
func maintainLeases(now time.Time, holder raft.NodeID, duration, renewBefore time.Duration, ranges ...leaseRange) {
	for _, r := range ranges {
		if role, _ := r.Status(); role != raft.Leader {
			continue
		}
		lease := r.CurrentLease()
		if lease.Holder == holder && lease.Expiration.After(now.Add(renewBefore)) {
			continue
		}
		_ = r.GrantLease(holder, duration)
	}
}

// splitCheckRange is the subset of kv.DurableRange checkSplitRecommendations needs.
type splitCheckRange interface {
	MaybeSplitKey(threshold int) ([]byte, bool, error)
}

// checkSplitRecommendations runs kv.ShouldSplit's decision (via MaybeSplitKey) against
// every named range and records the result as a gauge -- the piece
// docs/notes/12-split-repair.md names as still missing: "nothing calls MaybeSplitKey on a
// timer." This is still the decision only: nothing here executes a split. A rebuild-from-
// scratch live split needs to stand up fresh child Raft groups at runtime (new listeners,
// new storage directories, new IDs), which is real, separate orchestration work this
// binary does not attempt -- see that same doc for the full accounting of what's proven
// (the migration itself, KV and vector) versus what's still just a per-range signal here.
// AllKeys is a full scan of the range's applied data (see its own doc comment), so this is
// deliberately checked on a slower, separate cadence from Raft ticking and closed-timestamp
// advancement, not on every tick.
func checkSplitRecommendations(threshold int, gauge *prometheus.GaugeVec, ranges map[string]splitCheckRange) {
	for rangeID, r := range ranges {
		value := 0.0
		if _, recommended, err := r.MaybeSplitKey(threshold); err == nil && recommended {
			value = 1
		}
		gauge.WithLabelValues(rangeID).Set(value)
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	fatal := func(message string, args ...any) {
		slog.Error(message, args...)
		os.Exit(1)
	}
	id := flag.Uint64("id", 0, "this node's Raft ID (must be a key in --peers)")
	peersFlag := flag.String("peers", "", `Raft group members and their transport addresses, e.g. "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003" -- identical on every node in the deployment`)
	learnersFlag := flag.String("learners", "", "comma-separated non-voting Raft IDs from --peers; use while a replica catches up before promotion")
	dataDir := flag.String("data-dir", "", "on-disk directory for this node's storage engine and Raft log (required, unique per node)")
	grpcListen := flag.String("grpc-listen", ":8080", "client-facing gRPC listen address")
	dimension := flag.Int("dimension", 3, "fixed collection vector dimension")
	metricsListen := flag.String("metrics-listen", ":9090", "Prometheus metrics listen address")
	tickInterval := flag.Duration("tick-interval", 50*time.Millisecond, "how often this node advances its Raft clock")
	kvSplitKey := flag.String("kv-split-key", "m", "static split key for the two durable transactional KV ranges")
	closedTimestampInterval := flag.Duration("closed-timestamp-interval", 500*time.Millisecond, "how often a KV range leader proposes an advanced closed timestamp (see kv.DurableRange.AdvanceClosedTimestamp)")
	closedTimestampLag := flag.Duration("closed-timestamp-lag", time.Second, "how far behind wall-clock now each advanced closed timestamp is set -- must exceed closed-timestamp-interval plus real replication latency, or a legitimate in-flight read could exceed the promise before it even reaches a follower")
	splitCheckInterval := flag.Duration("split-check-interval", 5*time.Second, "how often each KV range's key count is checked against --split-threshold (see kv.ShouldSplit) -- decision only, does not execute a split")
	splitThreshold := flag.Int("split-threshold", 100000, "key count above which a KV range is reported as recommending a split (consensa_kv_split_recommended)")
	leaseDuration := flag.Duration("lease-duration", 6*time.Second, "how long an automatically granted follower-read lease is valid for once committed")
	leaseRenewBefore := flag.Duration("lease-renew-before", 3*time.Second, "renew a range's lease once less than this much validity remains, so a valid lease exists continuously rather than lapsing between grants")
	flag.Parse()

	if *id == 0 {
		fatal("invalid startup configuration", "reason", "--id is required and must be nonzero")
	}
	if *dataDir == "" {
		fatal("invalid startup configuration", "reason", "--data-dir is required")
	}
	if *kvSplitKey == "" {
		fatal("invalid startup configuration", "reason", "--kv-split-key must not be empty")
	}

	allPeers, err := parsePeers(*peersFlag)
	if err != nil {
		fatal("invalid peer configuration", "error", err)
	}
	learners, err := parseLearners(*learnersFlag, allPeers)
	if err != nil {
		fatal("invalid learner configuration", "error", err)
	}
	selfID := raft.NodeID(*id)
	selfAddr, ok := allPeers[selfID]
	if !ok {
		fatal("node absent from peer configuration", "node_id", selfID)
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

	// Every local Raft group registers a logical transport view on this one listener.
	// A deployment with vectors plus two KV ranges therefore still has exactly one peer
	// socket per process; range IDs are carried inside the multiplexed transport envelope.
	transport, err := raft.ListenMultiplexed(selfID, selfAddr)
	if err != nil {
		fatal("starting shared raft transport", "error", err)
	}
	defer func() {
		if err := transport.Close(); err != nil {
			slog.Error("closing shared raft transport", "error", err)
		}
	}()

	node, err := ann.NewDurableNode(ann.DurableNodeConfig{
		ID: selfID, GroupPeers: groupPeers, Learners: learners, ListenAddress: selfAddr, TransportPeers: transportPeers,
		Transport:  transport.Register(0, transportPeers),
		StorageDir: *dataDir, Index: ann.Config{Dimension: *dimension, M: 16, EFConstruction: 64, EFSearch: 64, Seed: 1},
	})
	if err != nil {
		fatal("starting durable node", "error", err)
	}
	defer func() {
		if err := node.Close(); err != nil {
			slog.Error("closing durable node", "error", err)
		}
	}()

	newKVRange := func(rangeID uint64) *kv.DurableRange {
		rangeNode, err := kv.NewDurableRange(kv.DurableRangeConfig{
			ID: selfID, GroupPeers: groupPeers, Learners: learners, ListenAddress: selfAddr, TransportPeers: transportPeers,
			Transport:  transport.Register(rangeID, transportPeers),
			StorageDir: filepath.Join(*dataDir, "kv", fmt.Sprintf("range-%d", rangeID)),
		})
		if err != nil {
			fatal("starting durable KV range", "range_id", rangeID, "error", err)
		}
		return rangeNode
	}
	leftRange := newKVRange(1)
	defer func() {
		if err := leftRange.Close(); err != nil {
			slog.Error("closing durable KV range", "range_id", 1, "error", err)
		}
	}()
	rightRange := newKVRange(2)
	defer func() {
		if err := rightRange.Close(); err != nil {
			slog.Error("closing durable KV range", "range_id", 2, "error", err)
		}
	}()
	meta, err := kv.NewMeta([]kv.Descriptor{
		{ID: 1, Start: nil, End: []byte(*kvSplitKey), Replicas: groupPeers},
		{ID: 2, Start: []byte(*kvSplitKey), End: nil, Replicas: groupPeers},
	})
	if err != nil {
		fatal("creating KV range descriptors", "error", err)
	}
	kvService := server.NewKVService(
		kv.NewRouter(meta),
		txn.NewCoordinator(txn.NewClock(time.Now)),
		map[uint64]txn.Participant{1: txn.NewDurableStore(leftRange), 2: txn.NewDurableStore(rightRange)},
	)
	adminService := server.NewAdminService(map[uint64]server.MembershipTarget{1: leftRange, 2: rightRange})

	metricRegistry := metrics.NewRegistry()

	// Raft only makes progress when something drives its clock; production wires that to
	// a real timer instead of the deterministic simulator's stepped scheduler tests use.
	//
	// Closed-timestamp advancement (docs/notes/09-leases.md) is folded into this SAME
	// loop and goroutine, on a tick-count gate, rather than its own separate ticker --
	// deliberately, not for tidiness. A second goroutine independently calling Propose
	// against the same *raft.Host instances this loop already ticks doubles concurrent
	// pressure on Host's own internal mutex, which is held across a blocking network send
	// (transport.Send inside driveLocked, host.go) -- found as a real regression in
	// cmd/consensa's own three-process end-to-end test: a separate closed-timestamp
	// goroutine at 500ms measurably destabilized leadership that the single-goroutine
	// version never did, even after fixing an unrelated real O(log length) cost in
	// raftLog.term (see that fix's own commit). One goroutine serializing everything it
	// proposes is what keeps this safe.
	closedTimestampEveryNTicks := int(*closedTimestampInterval / *tickInterval)
	if closedTimestampEveryNTicks < 1 {
		closedTimestampEveryNTicks = 1
	}
	stopTicking := make(chan struct{})
	go func() {
		ticker := time.NewTicker(*tickInterval)
		defer ticker.Stop()
		ticks := 0
		for {
			select {
			case <-stopTicking:
				return
			case <-ticker.C:
				// A tick error here means "not leader" or a dial failure to a peer that
				// is temporarily down -- both are ordinary and handled by internal/raft's
				// own retry-on-next-heartbeat behavior, not something this loop must react to.
				_ = node.Tick()
				_ = leftRange.Tick()
				_ = rightRange.Tick()
				ticks++
				if ticks%closedTimestampEveryNTicks == 0 {
					advanceClosedTimestamps(time.Now(), *closedTimestampLag, leftRange, rightRange)
					maintainLeases(time.Now(), raft.NodeID(*id), *leaseDuration, *leaseRenewBefore, leftRange, rightRange)
				}
				// Recall is reported separately by an external benchmark through
				// /report-recall. RangeQPS is set separately below, since it is a rate over
				// a fixed window rather than an instantaneous value like the Raft term.
				_, term, _ := node.Status()
				metricRegistry.RaftTerm.Set(float64(term))
			}
		}
	}()
	defer close(stopTicking)

	// A separate goroutine, unlike closed-timestamp advancement above: MaybeSplitKey only
	// reads this replica's own storage.Engine directly (AllKeys, durable_range.go) and
	// never calls Host.Propose, so it never contends for the same mutex the tick loop
	// holds across a blocking network send -- the specific hazard that made the
	// closed-timestamp check unsafe as a second goroutine. AllKeys is a full scan, so
	// running it on its own slower cadence (default 5s) here, off the tick loop entirely,
	// also keeps a large range's scan from ever delaying real-time Raft ticking.
	stopSplitCheck := make(chan struct{})
	go func() {
		ticker := time.NewTicker(*splitCheckInterval)
		defer ticker.Stop()
		ranges := map[string]splitCheckRange{"1": leftRange, "2": rightRange}
		for {
			select {
			case <-stopSplitCheck:
				return
			case <-ticker.C:
				checkSplitRecommendations(*splitThreshold, metricRegistry.SplitRecommended, ranges)
			}
		}
	}()
	defer close(stopSplitCheck)

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
			slog.Error("metrics server stopped", "error", err)
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
		fatal("starting gRPC listener", "error", err, "address", *grpcListen)
	}
	grpcServer := grpc.NewServer()
	consensav1.RegisterConsensaServer(grpcServer, service)
	consensav1.RegisterConsensaKVServer(grpcServer, kvService)
	consensav1.RegisterConsensaAdminServer(grpcServer, adminService)
	slog.Info("consensa node started", "node_id", selfID, "raft_address", selfAddr, "grpc_address", *grpcListen, "metrics_address", *metricsListen, "data_dir", *dataDir, "kv_split_key", *kvSplitKey, "raft_groups", 3)
	if err := grpcServer.Serve(listener); err != nil {
		fatal("gRPC server stopped", "error", err)
	}
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

// parseLearners keeps startup membership strict: every learner must be a peer that has a
// transport address in the same deployment configuration. The empty flag means all peers
// start as voters.
func parseLearners(raw string, peers map[raft.NodeID]string) ([]raft.NodeID, error) {
	if raw == "" {
		return nil, nil
	}
	seen := map[raft.NodeID]bool{}
	var learners []raft.NodeID
	for _, part := range strings.Split(raw, ",") {
		id, err := strconv.ParseUint(part, 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("malformed learner ID %q", part)
		}
		nodeID := raft.NodeID(id)
		if _, ok := peers[nodeID]; !ok {
			return nil, fmt.Errorf("learner %d is absent from --peers", nodeID)
		}
		if !seen[nodeID] {
			seen[nodeID] = true
			learners = append(learners, nodeID)
		}
	}
	if len(learners) >= len(peers) {
		return nil, fmt.Errorf("at least one peer must be a voter")
	}
	return learners, nil
}
