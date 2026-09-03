package metrics

import "testing"

// TestRegistryGathersSignals proves the default operational surface can be scraped.
//
// 5, not the full 8 metrics NewRegistry registers: consensa_kv_split_recommended and
// consensa_kv_split_executed_total are GaugeVec/CounterVec, and a Vec with no label
// combination ever Set/Inc'd contributes no series to Gather() -- so the always-present
// families are RaftTerm, RangeQPS, Recall, RaftElections, SearchLatency, and (once
// TxnCommits below is exercised) consensa_txn_commits_total, totalling 6. RaftTerm.Set is
// kept as a representative example that a plain Gauge always gathers even before this
// test touches it; TxnCommits additionally needs a label combination incremented since
// it is itself a Vec, same as the two split metrics this comment explains.
func TestRegistryGathersSignals(t *testing.T) {
	m := NewRegistry()
	m.RaftTerm.Set(2)
	m.TxnCommits.WithLabelValues("success").Inc()
	families, e := m.Registry.Gather()
	if e != nil || len(families) != 6 {
		t.Fatalf("gather=%d,%v", len(families), e)
	}
}

// TestSearchLatencyAndElectionsAreRealMetrics proves the two new non-Vec metrics this
// task added are wired into the registry and observable, the way RaftTerm already was.
func TestSearchLatencyAndElectionsAreRealMetrics(t *testing.T) {
	m := NewRegistry()
	m.SearchLatency.Observe(0.01)
	m.RaftElections.Inc()
	families, e := m.Registry.Gather()
	if e != nil {
		t.Fatalf("gather: %v", e)
	}
	var sawLatency, sawElections bool
	for _, f := range families {
		switch f.GetName() {
		case "consensa_search_latency_seconds":
			sawLatency = f.GetMetric()[0].GetHistogram().GetSampleCount() == 1
		case "consensa_raft_elections_total":
			sawElections = f.GetMetric()[0].GetCounter().GetValue() == 1
		}
	}
	if !sawLatency || !sawElections {
		t.Fatalf("sawLatency=%v sawElections=%v", sawLatency, sawElections)
	}
}
