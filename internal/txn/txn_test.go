package txn

import (
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/metrics"
)

// TestHLCObservePreservesCausality proves receiving a future timestamp cannot be followed by an earlier one.
func TestHLCObservePreservesCausality(t *testing.T) {
	c := NewClock(func() time.Time { return time.Unix(0, 1) })
	seen := Timestamp{WallTime: 100, Logical: 5}
	if got := c.Observe(seen); got.Compare(seen) <= 0 {
		t.Fatalf("observed %v not after %v", got, seen)
	}
}

// TestCoordinatorCommitsAcrossParticipants proves committed values become visible together.
func TestCoordinatorCommitsAcrossParticipants(t *testing.T) {
	clock := NewClock(time.Now)
	a, b := NewStore(), NewStore()
	e := NewCoordinator(clock).Commit("t1", map[Participant][]Intent{a: {{Key: []byte("a"), Value: []byte("1")}}, b: {{Key: []byte("b"), Value: []byte("2")}}})
	if e != nil {
		t.Fatal(e)
	}
	if v, _ := a.Get([]byte("a")); string(v) != "1" {
		t.Fatal("missing a")
	}
	if v, _ := b.Get([]byte("b")); string(v) != "2" {
		t.Fatal("missing b")
	}
}

// TestCoordinatorCommitRecordsMetricsByOutcome proves consensa_txn_commits_total (added
// for the Grafana transaction panel) is actually incremented by a real Commit call --
// once for a success and once for a failure, each landing under its own "outcome" label,
// not just present on the Registry with no caller ever exercising the Inc call in
// Coordinator.Commit.
func TestCoordinatorCommitRecordsMetricsByOutcome(t *testing.T) {
	clock := NewClock(time.Now)
	reg := metrics.NewRegistry()

	ok := NewCoordinator(clock)
	ok.SetMetrics(reg)
	store := NewStore()
	if err := ok.Commit("ok", map[Participant][]Intent{store: {{Key: []byte("k"), Value: []byte("v")}}}); err != nil {
		t.Fatal(err)
	}

	failing := NewCoordinator(clock)
	failing.SetMetrics(reg)
	// A nil Participant has no valid Record/WriteIntent behavior, so Prepare fails and
	// Commit returns a non-nil error -- the "failure" outcome this test needs, without
	// depending on any specific internal error path.
	if err := failing.Commit("fail", map[Participant][]Intent{nil: {{Key: []byte("k"), Value: []byte("v")}}}); err == nil {
		t.Fatal("expected Commit against a nil participant to fail")
	}

	if got := testutilCounterValue(t, reg, "consensa_txn_commits_total", "success"); got != 1 {
		t.Fatalf("success commits = %v, want 1", got)
	}
	if got := testutilCounterValue(t, reg, "consensa_txn_commits_total", "failure"); got != 1 {
		t.Fatalf("failure commits = %v, want 1", got)
	}
}

// testutilCounterValue reads one label value off a CounterVec family by scanning
// Gather() output directly, matching this package's existing style (no
// prometheus/client_golang/testutil dependency) rather than introducing a new one just
// for this test.
func testutilCounterValue(t *testing.T, reg *metrics.Registry, family, outcome string) float64 {
	t.Helper()
	families, err := reg.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != family {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "outcome" && l.GetValue() == outcome {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestCoordinatorAbortsPartiallyPreparedParticipant proves a conflict after an earlier
// participant intent does not leave that earlier intent stranded.
func TestCoordinatorAbortsPartiallyPreparedParticipant(t *testing.T) {
	clock := NewClock(time.Now)
	store := NewStore()
	if err := store.WriteIntent(Intent{Key: []byte("conflict"), TxnID: "other"}); err != nil {
		t.Fatal(err)
	}
	err := NewCoordinator(clock).Commit("t1", map[Participant][]Intent{store: {{Key: []byte("clean"), Value: []byte("1")}, {Key: []byte("conflict"), Value: []byte("2")}}})
	if err == nil {
		t.Fatal("conflicting transaction committed")
	}
	if _, stuck := store.intents["clean"]; stuck {
		t.Fatal("partial intent remained after abort")
	}
}

// TestCommittedRecordSurvivesCoordinatorCrashBeforeResolution proves the anchor record
// is sufficient to finish intent cleanup after the original coordinator stops mid-commit.
func TestCommittedRecordSurvivesCoordinatorCrashBeforeResolution(t *testing.T) {
	clock := NewClock(time.Now)
	anchor, participant := NewStore(), NewStore()
	coordinator := NewCoordinator(clock)
	txn, err := coordinator.Prepare("t-crash", []WriteSet{
		{Store: anchor, Intents: []Intent{{Key: []byte("a"), Value: []byte("1")}}},
		{Store: participant, Intents: []Intent{{Key: []byte("b"), Value: []byte("2")}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitRecord(txn); err != nil {
		t.Fatal(err)
	}
	// This is the crash window: no cleanup has happened, so a naive read sees an intent.
	if _, err := participant.Get([]byte("b")); err == nil {
		t.Fatal("participant exposed a value before intent resolution")
	}
	lookup := func(id string) (Record, bool) { return anchor.Record(id) }
	for store, key := range map[*Store]string{anchor: "a", participant: "b"} {
		value, err := store.Read([]byte(key), lookup)
		if err != nil || string(value) == "" {
			t.Fatalf("resolved Read(%q) = %q, %v", key, value, err)
		}
	}
	if err := NewCoordinator(clock).Resolve(txn); err != nil {
		t.Fatal(err)
	}
	for store, key := range map[*Store]string{anchor: "a", participant: "b"} {
		value, err := store.Get([]byte(key))
		if err != nil || string(value) == "" {
			t.Fatalf("Get(%q) = %q, %v", key, value, err)
		}
	}
}

// TestReadHidesAbortedIntent proves a reader consults the authoritative record instead
// of treating a provisional value as visible when a transaction has already aborted.
func TestReadHidesAbortedIntent(t *testing.T) {
	store := NewStore()
	if err := store.WriteIntent(Intent{Key: []byte("key"), Value: []byte("provisional"), TxnID: "aborted"}); err != nil {
		t.Fatal(err)
	}
	lookup := func(id string) (Record, bool) {
		if id != "aborted" {
			return Record{}, false
		}
		return Record{ID: id, Status: Aborted}, true
	}
	if _, err := store.Read([]byte("key"), lookup); err == nil {
		t.Fatal("aborted intent became visible")
	}
}
