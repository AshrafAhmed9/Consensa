package metrics

import "github.com/prometheus/client_golang/prometheus"

// Registry bundles per-node operational signals for HTTP exposition.
type Registry struct {
	Registry *prometheus.Registry
	RaftTerm prometheus.Gauge
	RangeQPS prometheus.Gauge
	Recall   prometheus.Gauge
}

// NewRegistry creates an isolated registry, useful for both one node and deterministic tests.
func NewRegistry() *Registry {
	r := prometheus.NewRegistry()
	term := prometheus.NewGauge(prometheus.GaugeOpts{Name: "consensa_raft_term", Help: "Current Raft term."})
	qps := prometheus.NewGauge(prometheus.GaugeOpts{Name: "consensa_range_qps", Help: "Observed requests per second for a range."})
	recall := prometheus.NewGauge(prometheus.GaugeOpts{Name: "consensa_ann_recall", Help: "Most recently measured ANN recall."})
	r.MustRegister(term, qps, recall)
	return &Registry{Registry: r, RaftTerm: term, RangeQPS: qps, Recall: recall}
}
