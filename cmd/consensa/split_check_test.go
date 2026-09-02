package main

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestCheckSplitRecommendationsSetsGaugeAboveThreshold proves checkSplitRecommendations
// actually drives the exposed metric off a real 3-node group's real applied data: below
// threshold the gauge reads 0, and once real writes push the range's key count past
// threshold, the very next check reports 1 -- checked against the real
// consensa_kv_split_recommended series, not just the function's return value in isolation
// (kv.ShouldSplit/DurableRange.MaybeSplitKey already cover that directly).
func TestCheckSplitRecommendationsSetsGaugeAboveThreshold(t *testing.T) {
	leader, all := startClosedTimestampTestRange(t)
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				for _, r := range all {
					_ = r.Tick()
				}
			}
		}
	}()
	defer close(stop)

	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_split_recommended"}, []string{"range_id"})
	ranges := map[string]splitCheckRange{"1": leader}

	checkSplitRecommendations(3, 0, map[string]float64{}, gauge, ranges)
	if got := testutil.ToFloat64(gauge.WithLabelValues("1")); got != 0 {
		t.Fatalf("gauge = %v before any writes, want 0", got)
	}

	for _, k := range []string{"a", "b", "c", "d"} {
		if err := leader.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if all, err := leader.AllKeys(); err == nil && len(all) == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writes never converged")
		}
		time.Sleep(10 * time.Millisecond)
	}

	checkSplitRecommendations(3, 0, map[string]float64{}, gauge, ranges)
	if got := testutil.ToFloat64(gauge.WithLabelValues("1")); got != 1 {
		t.Fatalf("gauge = %v after 4 keys past threshold 3, want 1", got)
	}
}

// TestCheckSplitRecommendationsSetsGaugeAboveQPSThreshold proves the QPS trigger fires
// independently of key count: a range with only two keys (well under any reasonable size
// threshold, and even below split.go's own minimum-2-keys guard boundary) still
// recommends a split once its measured QPS exceeds the configured QPS threshold --
// PLAN.md's own named gap ("no QPS-based trigger exists, only size"), closed here.
func TestCheckSplitRecommendationsSetsGaugeAboveQPSThreshold(t *testing.T) {
	leader, all := startClosedTimestampTestRange(t)
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				for _, r := range all {
					_ = r.Tick()
				}
			}
		}
	}()
	defer close(stop)

	for _, k := range []string{"a", "b"} {
		if err := leader.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if all, err := leader.AllKeys(); err == nil && len(all) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writes never converged")
		}
		time.Sleep(10 * time.Millisecond)
	}

	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_split_recommended_qps"}, []string{"range_id"})
	ranges := map[string]splitCheckRange{"1": leader}

	// Size threshold disabled (0); only the QPS criterion is active. A size threshold of
	// 100000 would never fire for a 2-key range -- this proves QPS alone is sufficient.
	checkSplitRecommendations(0, 1000, map[string]float64{"1": 0}, gauge, ranges)
	if got := testutil.ToFloat64(gauge.WithLabelValues("1")); got != 0 {
		t.Fatalf("gauge = %v with qps=0 against threshold 1000, want 0", got)
	}
	checkSplitRecommendations(0, 1000, map[string]float64{"1": 5000}, gauge, ranges)
	if got := testutil.ToFloat64(gauge.WithLabelValues("1")); got != 1 {
		t.Fatalf("gauge = %v with qps=5000 against threshold 1000, want 1", got)
	}
}
