package metrics

import "github.com/prometheus/client_golang/prometheus"

// Registry bundles per-node operational signals for HTTP exposition.
type Registry struct {
	Registry         *prometheus.Registry
	RaftTerm         prometheus.Gauge
	RangeQPS         prometheus.Gauge
	Recall           prometheus.Gauge
	SplitRecommended *prometheus.GaugeVec
	SplitExecuted    *prometheus.CounterVec
	MergeExecuted    *prometheus.CounterVec
	RaftElections    prometheus.Counter
	SearchLatency    prometheus.Histogram
	TxnCommits       *prometheus.CounterVec
}

// NewRegistry creates an isolated registry, useful for both one node and deterministic tests.
func NewRegistry() *Registry {
	r := prometheus.NewRegistry()
	term := prometheus.NewGauge(prometheus.GaugeOpts{Name: "consensa_raft_term", Help: "Current Raft term."})
	qps := prometheus.NewGauge(prometheus.GaugeOpts{Name: "consensa_range_qps", Help: "Observed requests per second for a range."})
	recall := prometheus.NewGauge(prometheus.GaugeOpts{Name: "consensa_ann_recall", Help: "Most recently measured ANN recall."})
	// 1 when kv.ShouldSplit's decision (kv.DurableRange.MaybeSplitKey) currently
	// recommends splitting this range, 0 otherwise. This is the DECISION only -- nothing
	// executes a split automatically off this signal yet, see docs/notes/12-split-repair.md.
	splitRecommended := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "consensa_kv_split_recommended",
		Help: "1 if this range's key count currently exceeds the split threshold, 0 otherwise.",
	}, []string{"range_id"})
	// Incremented once, by whichever process's own local replica actually completes the
	// migration (cmd/consensa's executeSplitIfRecommended), when a live split of
	// parent_range_id into left_range_id/right_range_id finishes -- the execution signal
	// consensa_kv_split_recommended above deliberately never claimed to be, since that
	// gauge only ever reported the decision.
	splitExecuted := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "consensa_kv_split_executed_total",
		Help: "Incremented once a live split of parent_range_id into left_range_id/right_range_id has actually completed.",
	}, []string{"parent_range_id", "left_range_id", "right_range_id"})
	mergeExecuted := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "consensa_range_merge_executed_total",
		Help: "Incremented once a cold sibling range has been absorbed and routing cut over.",
	}, []string{"parent_range_id", "surviving_range_id", "absorbed_range_id", "plane"})
	// Incremented once, in cmd/consensa's own Raft tick loop, each time this node's local
	// view of leadership (ann.DurableNode.Status's isLeader) transitions from false to
	// true -- a real election win, not just a term bump (a term can advance without this
	// node ever becoming leader). Kept a plain Counter rather than pushing the detection
	// into internal/raft: the tick loop already samples Status() every tick for
	// consensa_raft_term, so diffing consecutive isLeader values there is the same
	// zero-blast-radius pattern that metric already uses.
	elections := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consensa_raft_elections_total",
		Help: "Incremented each time this node's local Raft view transitions to leader.",
	})
	// consensa_search_latency_seconds times server.Service.Search end-to-end, from the
	// first line of the RPC handler to its final SendAndClose/return -- the closest real
	// stand-in for PLAN.md's "search latency histograms" panel, since no other timing
	// signal for the Search path exists in this codebase yet.
	searchLatency := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "consensa_search_latency_seconds",
		Help:    "Latency of the Search RPC handler, from entry to completion.",
		Buckets: prometheus.DefBuckets,
	})
	// consensa_txn_commits_total counts every internal/txn.Coordinator.Commit call by
	// outcome ("success" or "failure"), labeled the same way splitExecuted labels its own
	// outcome dimension above.
	txnCommits := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "consensa_txn_commits_total",
		Help: "Transactions committed via txn.Coordinator.Commit, by outcome.",
	}, []string{"outcome"})
	r.MustRegister(term, qps, recall, splitRecommended, splitExecuted, mergeExecuted, elections, searchLatency, txnCommits)
	return &Registry{
		Registry:         r,
		RaftTerm:         term,
		RangeQPS:         qps,
		Recall:           recall,
		SplitRecommended: splitRecommended,
		SplitExecuted:    splitExecuted,
		MergeExecuted:    mergeExecuted,
		RaftElections:    elections,
		SearchLatency:    searchLatency,
		TxnCommits:       txnCommits,
	}
}
