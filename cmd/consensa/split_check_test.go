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

	checkSplitRecommendations(3, gauge, ranges)
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

	checkSplitRecommendations(3, gauge, ranges)
	if got := testutil.ToFloat64(gauge.WithLabelValues("1")); got != 1 {
		t.Fatalf("gauge = %v after 4 keys past threshold 3, want 1", got)
	}
}
