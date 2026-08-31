package storage

import "github.com/prometheus/client_golang/prometheus"

// metrics are package-global because Prometheus collectors may be registered only once
// per process. Labels preserve operation-specific counts without creating per-key series.
var metrics = struct {
	operations *prometheus.CounterVec
	latency    *prometheus.HistogramVec
}{
	operations: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consensa_storage_operations_total", Help: "Completed storage operations by kind."}, []string{"operation"}),
	latency:    prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "consensa_storage_operation_seconds", Help: "Storage operation latency by kind."}, []string{"operation"}),
}

func init() { prometheus.MustRegister(metrics.operations, metrics.latency) }
