package main

import (
	"testing"
	"time"
)

// TestQPSTrackerFirstSampleReportsZero proves the first rate() call for a range ID never
// fabricates a rate from nothing -- there is no prior baseline to diff against yet,
// matching the pre-existing consensa_range_qps sampling loop's own "first window reports
// nothing" behavior.
func TestQPSTrackerFirstSampleReportsZero(t *testing.T) {
	tr := newQPSTracker()
	if got := tr.rate("1", 100); got != 0 {
		t.Fatalf("first rate() call = %v, want 0", got)
	}
}

// TestQPSTrackerComputesRateFromDelta proves a second call for the same range ID reports
// the real (count delta)/(time delta) rate, not just a nonzero placeholder.
func TestQPSTrackerComputesRateFromDelta(t *testing.T) {
	tr := newQPSTracker()
	tr.rate("1", 100)
	time.Sleep(50 * time.Millisecond)
	got := tr.rate("1", 600)
	// 500 requests over ~50ms is roughly 10000/sec; allow generous slack for scheduling
	// jitter in a test environment rather than asserting an exact value.
	if got < 5000 || got > 20000 {
		t.Fatalf("rate() = %v, want roughly 10000 (500 requests / ~50ms)", got)
	}
}

// TestQPSTrackerIsolatesDifferentRangeIDs proves one range's baseline never leaks into
// another's -- a live split's fresh child range must get its own independent first
// sample, not a stale rate borrowed from an unrelated range checked earlier in the same
// tick (see qpsTracker's own doc comment for why this matters).
func TestQPSTrackerIsolatesDifferentRangeIDs(t *testing.T) {
	tr := newQPSTracker()
	tr.rate("1", 100)
	if got := tr.rate("2", 5000); got != 0 {
		t.Fatalf("first rate() call for a NEW range ID = %v, want 0 (no baseline yet), even though range \"1\" already has one", got)
	}
}

// TestQPSTrackerHandlesCounterNotAdvancing proves a repeated call with an unchanged (or
// somehow decreased, which should never happen for a monotonic atomic counter but is
// checked defensively) count reports 0 rather than a negative or NaN rate.
func TestQPSTrackerHandlesCounterNotAdvancing(t *testing.T) {
	tr := newQPSTracker()
	tr.rate("1", 100)
	time.Sleep(10 * time.Millisecond)
	if got := tr.rate("1", 100); got != 0 {
		t.Fatalf("rate() with an unchanged count = %v, want 0", got)
	}
	time.Sleep(10 * time.Millisecond)
	if got := tr.rate("1", 50); got != 0 {
		t.Fatalf("rate() with a DECREASED count = %v, want 0, not a negative rate", got)
	}
}
