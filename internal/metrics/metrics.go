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
	r.MustRegister(term, qps, recall, splitRecommended, splitExecuted)
	return &Registry{Registry: r, RaftTerm: term, RangeQPS: qps, Recall: recall, SplitRecommended: splitRecommended, SplitExecuted: splitExecuted}
}
