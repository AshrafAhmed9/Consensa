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
	"sync"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/ann"
	"github.com/ashraf/consensa/internal/kv"
	"github.com/ashraf/consensa/internal/metrics"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/server"
	"github.com/ashraf/consensa/internal/txn"
	"github.com/ashraf/consensa/internal/vector"
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

// maintainLeadershipAffinity is docs/bugs/003's real fix, not just the electionStaggerSpread
// mitigation already landed for it (internal/raft/host.go). cmd/consensa hosts three
// independently-elected Raft groups per process -- the vector index and two KV ranges --
// and KVService.TransactionalPut can only commit a transaction spanning both KV ranges
// when the SAME process leads every range it touches, since nothing forwards a write to
// another process's leader (docs/notes/05-api.md rules that out as a general feature).
// hostElectionStagger already biases every group's own election toward the same node --
// the lowest-ranked ID in the shared peer list, identical across all three groups since
// they share one peer set -- but that bias is only a head start against real network
// jitter between independently-timed elections, not a guarantee: under enough scheduling
// variance the groups can settle into a stable split with no further election churn to
// self-correct it.
//
// This closes that gap without any new inter-process signaling or RPC surface: if THIS
// process currently leads a group but is not itself the preferred (lowest-ranked) node,
// it proactively calls TransferLeadershipTo the preferred node -- raft.Host's own
// MsgTimeoutNow primitive, the same mechanism etcd calls leadership transfer, extended
// here from a manual admin operation into a self-correcting background policy. Every
// process runs the identical check against the identical, deterministically-computed
// preferred ID, so this converges to the preferred node leading all three groups without
// any process needing to learn what group another process currently leads. A transfer
// that fails (the preferred node has not yet replicated up to this leader's last index)
// is silently retried on the next tick, the same pattern executeSplitIfRecommended and
// maintainLeases already use for their own not-yet-ready failures.
func maintainLeadershipAffinity(selfID, preferredID raft.NodeID, node *ann.DurableNode, leftRange, rightRange *kv.DurableRange) {
	if selfID == preferredID {
		return
	}
	if _, _, isLeader := node.Status(); isLeader {
		_ = node.TransferLeadershipTo(preferredID)
	}
	if role, _ := leftRange.Status(); role == raft.Leader {
		_ = leftRange.TransferLeadershipTo(preferredID)
	}
	if role, _ := rightRange.Status(); role == raft.Leader {
		_ = rightRange.TransferLeadershipTo(preferredID)
	}
}

// splitCheckRange is the subset of kv.DurableRange checkSplitRecommendations needs.
type splitCheckRange interface {
	MaybeSplitKey(threshold int) ([]byte, bool, error)
}

// checkSplitRecommendations runs kv.ShouldSplit's decision (via MaybeSplitKey) against
// every named range and records the result as a gauge -- purely the decision signal,
// updated regardless of whether execution has happened yet (see
// consensa_kv_split_executed_total, set only by executeSplitIfRecommended below, for the
// execution signal). AllKeys is a full scan of the range's applied data (see its own doc
// comment), so this is deliberately checked on a slower, separate cadence from Raft
// ticking and closed-timestamp advancement, not on every tick.
func checkSplitRecommendations(threshold int, gauge *prometheus.GaugeVec, ranges map[string]splitCheckRange) {
	for rangeID, r := range ranges {
		value := 0.0
		if _, recommended, err := r.MaybeSplitKey(threshold); err == nil && recommended {
			value = 1
		}
		gauge.WithLabelValues(rangeID).Set(value)
	}
}

// executeSplitTarget is the subset of kv.DurableRange executeSplitIfRecommended needs
// from the parent it is checking, beyond splitCheckRange's own MaybeSplitKey: AllKeys to
// read the applied data kv.ExecuteLiveSplit migrates.
type executeSplitTarget interface {
	splitCheckRange
	AllKeys() (map[string][]byte, error)
}

// executeSplitIfRecommended closes the gap checkSplitRecommendations' own doc comment
// used to name as still missing: "nothing here executes a split." It stands up two fresh
// child kv.DurableRange groups on THIS process's own already-shared MultiplexedTransport
// -- newChild is cmd/consensa's own newKVRange closure, reused unmodified, since
// registering a new range ID on an existing shared listener needs no new address or
// listener at all -- migrates the parent's applied data via kv.ExecuteLiveSplit, then
// publishes the new routing metadata and registers the fresh children with the running
// KVService/AdminService so they're immediately reachable, not just replicated.
//
// No cross-process coordination call is needed to decide WHEN to split: every process in
// the deployment runs this identical check against identical Raft-replicated applied
// state (Raft guarantees all three replicas of a range apply the same committed entries
// in the same order), so all three independently compute the identical split decision and
// split key, and each independently builds and registers its own local replica of the
// same two deterministic child IDs -- the same way the two original static ranges already
// start on all three processes without any handshake between them. Only the migration
// Put calls inside kv.ExecuteLiveSplit need a real leader, and its own retry already
// tolerates that not being this particular process at the moment it runs.
//
// executed guards against re-running the split on every future tick once it has
// succeeded once for this parentID -- required because MaybeSplitKey keeps recommending
// a split for as long as the parent's own (now-stale) key count stays past threshold; the
// parent range itself is deliberately left in place rather than deleted, matching
// docs/notes/12-split-repair.md's own stated simplification.
//
// inProgress caches the two child ranges across retries of the SAME parentID -- required
// because newChild opens each child's storage.Engine (via newKVRange, which calls this
// process's own fatal() and exits on a real open error, the same as leftRange/rightRange
// get at startup). A migration attempt that fails transiently (a child group's election
// hasn't settled yet, a network blip) must retry against the SAME already-open child
// objects on the next tick, not call newChild again for the same range ID: a second
// concurrent storage.Open against a directory the first attempt's Host is still actively
// using corrupts the WAL out from under it -- found as a real bug via
// TestConsensaBinaryExecutesALiveSplitAutomatically ("storage: invalid SSTable record"
// on the retry, then the whole process exiting via newKVRange's own fatal()).
func executeSplitIfRecommended(
	parentID uint64, parent executeSplitTarget, parentDescriptor kv.Descriptor, threshold int,
	newChild func(rangeID uint64) *kv.DurableRange,
	meta *kv.Meta, kvService *server.KVService, adminService *server.AdminService,
	startTicking func(*kv.DurableRange), splitExecutedCounter *prometheus.CounterVec,
	executed map[uint64]bool, inProgress map[uint64][2]*kv.DurableRange, mu *sync.Mutex,
) {
	mu.Lock()
	alreadyDone := executed[parentID]
	children, attemptStarted := inProgress[parentID]
	mu.Unlock()
	if alreadyDone {
		return
	}

	splitKey, recommended, err := parent.MaybeSplitKey(threshold)
	if err != nil || !recommended {
		return
	}
	parentData, err := parent.AllKeys()
	if err != nil {
		slog.Error("live split: reading parent data", "range_id", parentID, "error", err)
		return
	}

	leftID, rightID := parentID*10+1, parentID*10+2
	if !attemptStarted {
		left := newChild(leftID)
		right := newChild(rightID)
		startTicking(left)
		startTicking(right)
		children = [2]*kv.DurableRange{left, right}
		mu.Lock()
		inProgress[parentID] = children
		mu.Unlock()
	}
	left, right := children[0], children[1]

	// A short per-key timeout, not a generous one: this whole function is already
	// retried by the caller every --split-check-interval, so a slow per-key budget here
	// just makes one failed attempt (this process's local child replica isn't leader
	// this round) block for far longer than useful before the next scheduled retry gets
	// a chance -- with several keys, a 20s-per-key budget could keep one call blocked
	// for over a minute even though the real leader (a different process) might finish
	// in milliseconds. Found as a real CI failure: the split never completed within a
	// generous overall test deadline because each unsuccessful attempt was burning
	// nearly all of it on its own.
	leftDesc, rightDesc, err := kv.ExecuteLiveSplit(parentDescriptor, parentData, splitKey, leftID, rightID, left, right, 2*time.Second)
	if err != nil {
		slog.Error("live split: migration failed, will retry against the same child ranges next tick", "range_id", parentID, "error", err)
		return
	}

	next := meta.All()
	filtered := next[:0]
	for _, d := range next {
		if d.ID != parentID {
			filtered = append(filtered, d)
		}
	}
	filtered = append(filtered, leftDesc, rightDesc)
	if err := meta.Replace(filtered); err != nil {
		slog.Error("live split: publishing new routing metadata", "range_id", parentID, "error", err)
		return
	}

	kvService.RegisterStore(leftID, txn.NewDurableStore(left))
	kvService.RegisterStore(rightID, txn.NewDurableStore(right))
	adminService.RegisterRange(leftID, left)
	adminService.RegisterRange(rightID, right)

	mu.Lock()
	executed[parentID] = true
	delete(inProgress, parentID)
	mu.Unlock()
	splitExecutedCounter.WithLabelValues(fmt.Sprint(parentID), fmt.Sprint(leftID), fmt.Sprint(rightID)).Inc()
	slog.Info("live split executed", "parent_range_id", parentID, "left_range_id", leftID, "right_range_id", rightID, "split_key", string(splitKey))
}

// annExecuteSplitTarget is the vector-plane counterpart of executeSplitTarget: the subset
// of *ann.DurableNode executeAnnSplitIfRecommended needs from the parent it is checking.
type annExecuteSplitTarget interface {
	MaybeSplitKey(threshold int) (string, bool, error)
	AllVectors() map[string]vector.Vector
}

// executeAnnSplitIfRecommended is executeSplitIfRecommended's vector-plane counterpart --
// same structure, same reasoning (every process runs the identical check against
// identical Raft-replicated applied state, so no cross-process coordination call is
// needed to decide WHEN to split), built on ann.ExecuteLiveSplit instead of
// kv.ExecuteLiveSplit. See ann.ShouldSplit's own doc comment for the one real limitation
// this inherits: the split boundary is a lexicographic ID bisection, not a clustering-
// aware vector-space boundary, so recall near that boundary can dip until each child's own
// graph structure compensates -- deliberately the minimum viable decision proving
// automatic execution works end-to-end, not PLAN.md's Phase 10 answer to HNSW-under-splits.
//
// leftID/rightID use *100, not KV's *10, so a vector-plane split's transport-multiplexed
// range IDs (registered on the SAME shared MultiplexedTransport the KV ranges and their
// own children use) can never collide with kv.DurableRange's own deterministic child IDs
// (parentID*10+1/+2) -- both planes' parent IDs currently happen to be 1, so *10 would
// otherwise produce identical transport IDs (11/12) for two entirely different Raft groups.
func executeAnnSplitIfRecommended(
	parentID uint64, parent annExecuteSplitTarget, parentDescriptor ann.Descriptor, threshold int,
	newChild func(rangeID uint64) *ann.DurableNode,
	meta *ann.Meta, service *server.Service,
	startTicking func(*ann.DurableNode), splitExecutedCounter *prometheus.CounterVec,
	executed map[uint64]bool, inProgress map[uint64][2]*ann.DurableNode, mu *sync.Mutex,
) {
	mu.Lock()
	alreadyDone := executed[parentID]
	children, attemptStarted := inProgress[parentID]
	mu.Unlock()
	if alreadyDone {
		return
	}

	splitKey, recommended, err := parent.MaybeSplitKey(threshold)
	if err != nil || !recommended {
		return
	}
	parentVectors := parent.AllVectors()

	leftID, rightID := parentID*100+1, parentID*100+2
	if !attemptStarted {
		left := newChild(leftID)
		right := newChild(rightID)
		startTicking(left)
		startTicking(right)
		children = [2]*ann.DurableNode{left, right}
		mu.Lock()
		inProgress[parentID] = children
		mu.Unlock()
	}
	left, right := children[0], children[1]

	leftDesc, rightDesc, err := ann.ExecuteLiveSplit(parentDescriptor, parentVectors, splitKey, leftID, rightID, left, right, 2*time.Second)
	if err != nil {
		slog.Error("live split: migration failed, will retry against the same child ranges next tick", "plane", "vector", "range_id", parentID, "error", err)
		return
	}

	next := meta.All()
	filtered := next[:0]
	for _, d := range next {
		if d.ID != parentID {
			filtered = append(filtered, d)
		}
	}
	filtered = append(filtered, leftDesc, rightDesc)
	if err := meta.Replace(filtered); err != nil {
		slog.Error("live split: publishing new routing metadata", "plane", "vector", "range_id", parentID, "error", err)
		return
	}

	service.RegisterIndex(leftID, left)
	service.RegisterIndex(rightID, right)

	mu.Lock()
	executed[parentID] = true
	delete(inProgress, parentID)
	mu.Unlock()
	splitExecutedCounter.WithLabelValues(fmt.Sprint(parentID), fmt.Sprint(leftID), fmt.Sprint(rightID)).Inc()
	slog.Info("live split executed", "plane", "vector", "range_id", parentID, "left_range_id", leftID, "right_range_id", rightID, "split_key", splitKey)
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

	newAnnChild := func(rangeID uint64) *ann.DurableNode {
		child, err := ann.NewDurableNode(ann.DurableNodeConfig{
			ID: selfID, GroupPeers: groupPeers, Learners: learners, ListenAddress: selfAddr, TransportPeers: transportPeers,
			Transport:  transport.Register(rangeID, transportPeers),
			StorageDir: filepath.Join(*dataDir, "ann", fmt.Sprintf("range-%d", rangeID)),
			Index:      ann.Config{Dimension: *dimension, M: 16, EFConstruction: 64, EFSearch: 64, Seed: 1},
		})
		if err != nil {
			fatal("starting durable ann range", "range_id", rangeID, "error", err)
		}
		return child
	}

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
	leftDescriptor := kv.Descriptor{ID: 1, Start: nil, End: []byte(*kvSplitKey), Replicas: groupPeers}
	rightDescriptor := kv.Descriptor{ID: 2, Start: []byte(*kvSplitKey), End: nil, Replicas: groupPeers}
	meta, err := kv.NewMeta([]kv.Descriptor{leftDescriptor, rightDescriptor})
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

	// startTicking gives a range constructed after startup (a live split's fresh
	// children) the same real-timer-driven Tick() loop leftRange/rightRange get above,
	// stopping on the identical stopTicking channel so a child's ticking goroutine never
	// outlives the process. A dedicated goroutine per child, not a growable slice shared
	// with the main tick loop above: that loop's own slice is captured once at
	// construction, and racing a live append against its concurrent range-over read
	// would be exactly the kind of data race Meta/KVService/AdminService's own new
	// mutexes exist to avoid elsewhere in this file.
	startTicking := func(r *kv.DurableRange) {
		go func() {
			ticker := time.NewTicker(*tickInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stopTicking:
					return
				case <-ticker.C:
					_ = r.Tick()
				}
			}
		}()
	}

	// preferredLeader is the same node every group's own election already deterministically
	// favors (hostElectionStagger, internal/raft/host.go): the lowest-ranked ID in the
	// shared peer list, identical across the vector index and both KV ranges since they
	// all share one peer set. maintainLeadershipAffinity below uses it as the real fix for
	// docs/bugs/003, not just electionStaggerSpread's mitigation of it.
	preferredLeader := groupPeers[0]
	stopAffinity := make(chan struct{})
	go func() {
		ticker := time.NewTicker(*tickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopAffinity:
				return
			case <-ticker.C:
				maintainLeadershipAffinity(selfID, preferredLeader, node, leftRange, rightRange)
			}
		}
	}()
	defer close(stopAffinity)

	// A separate goroutine, unlike closed-timestamp advancement above: MaybeSplitKey only
	// reads this replica's own storage.Engine directly (AllKeys, durable_range.go) and
	// never calls Host.Propose, so it never contends for the same mutex the tick loop
	// holds across a blocking network send -- the specific hazard that made the
	// closed-timestamp check unsafe as a second goroutine. AllKeys is a full scan, so
	// running it on its own slower cadence (default 5s) here, off the tick loop entirely,
	// also keeps a large range's scan from ever delaying real-time Raft ticking. Real
	// split execution (executeSplitIfRecommended) runs on this same goroutine, not a
	// third one: it can block for up to several seconds migrating data, which is fine to
	// delay this goroutine's own next tick (splits are rare) but would be a real problem
	// on the main Raft tick loop above.
	splitExecuted := map[uint64]bool{}
	splitInProgress := map[uint64][2]*kv.DurableRange{}
	var splitMu sync.Mutex
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
				executeSplitIfRecommended(1, leftRange, leftDescriptor, *splitThreshold, newKVRange, meta, kvService, adminService, startTicking, metricRegistry.SplitExecuted, splitExecuted, splitInProgress, &splitMu)
				executeSplitIfRecommended(2, rightRange, rightDescriptor, *splitThreshold, newKVRange, meta, kvService, adminService, startTicking, metricRegistry.SplitExecuted, splitExecuted, splitInProgress, &splitMu)
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

	// startTickingAnn mirrors startTicking (above) for a live split's fresh vector-plane
	// children, reusing the identical stopTicking channel every other range's ticking
	// goroutine already stops on.
	startTickingAnn := func(a *ann.DurableNode) {
		go func() {
			ticker := time.NewTicker(*tickInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stopTicking:
					return
				case <-ticker.C:
					_ = a.Tick()
				}
			}
		}()
	}

	// vectorDescriptor matches server.NewService's own default single-range catalog
	// (ID 1, unbounded) exactly -- kept here, not read back from service.Meta(), because
	// executeAnnSplitIfRecommended needs the PARENT descriptor before any split has ever
	// run, and this is the only place that shape is otherwise implicit.
	vectorDescriptor := ann.Descriptor{ID: 1, Start: "", End: "", Replicas: groupPeers}
	annSplitExecuted := map[uint64]bool{}
	annSplitInProgress := map[uint64][2]*ann.DurableNode{}
	var annSplitMu sync.Mutex
	stopAnnSplitCheck := make(chan struct{})
	go func() {
		ticker := time.NewTicker(*splitCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopAnnSplitCheck:
				return
			case <-ticker.C:
				executeAnnSplitIfRecommended(1, node, vectorDescriptor, *splitThreshold, newAnnChild, service.Meta(), service, startTickingAnn, metricRegistry.SplitExecuted, annSplitExecuted, annSplitInProgress, &annSplitMu)
			}
		}
	}()
	defer close(stopAnnSplitCheck)

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
