package txn

import (
	"testing"
	"time"
)

// TestDurableStorePrepareRefreshesInsteadOfAborting proves read-refresh
// (docs/notes/14-serializable.md) closes for the real, Raft-replicated path too, not just
// the in-memory Store (TestPrepareRefreshesInsteadOfAbortingWhenPriorReadsStillHold,
// refresh_test.go): T1 starts before T2 but its Prepare call for "a" arrives after T2 has
// already recorded a later read of "a" over a real 3-node group -- the same
// ErrWriteBelowObservedRead conflict TestDurableStoreRejectsWriteSkew reproduces. T1 also
// has its own prior read of an unrelated key "b" that nothing has since written, so
// Coordinator.Prepare's refresh path must push T1's timestamp and commit it instead of
// aborting outright.
func TestDurableStorePrepareRefreshesInsteadOfAborting(t *testing.T) {
	rng := startDurableRange(t, 1)
	store := NewDurableStore(rng)

	seedAndConfirm := func(key, value string) {
		t.Helper()
		if err := rng.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if got, err := rng.Get([]byte(key)); err == nil && string(got) == value {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("seed %s never became visible", key)
	}
	seedAndConfirm("a", "on-call")
	seedAndConfirm("b", "on-call")

	t1, t2 := Timestamp{WallTime: 100}, Timestamp{WallTime: 200}

	// T1 reads "b" at t1 -- the read Prepare's refresh must re-validate.
	if v, err := rng.Get([]byte("b")); err != nil || string(v) != "on-call" {
		t.Fatalf("T1 read b = %q, %v", v, err)
	}

	// T2 reads "a" at t2, later than t1 -- the conflict T1's write to "a" will hit.
	if err := store.RecordRead([]byte("a"), t2); err != nil {
		t.Fatalf("RecordRead: %v", err)
	}

	clock := NewClock(func() time.Time { return time.Unix(0, t1.WallTime) })
	txn, err := NewCoordinator(clock).Prepare("t1", []WriteSet{{
		Store:   store,
		Intents: []Intent{{Key: []byte("a"), Value: []byte("off-call")}},
		Reads:   [][]byte{[]byte("b")},
	}})
	if err != nil {
		t.Fatalf("Prepare should have refreshed and committed over real Raft replication instead of aborting: %v", err)
	}
	if txn.Record.WriteTimestamp.Compare(t2) <= 0 {
		t.Fatalf("write timestamp %v was not pushed past the conflicting read %v", txn.Record.WriteTimestamp, t2)
	}
}

// TestDurableStorePrepareAbortsWhenRefreshFindsAStaleRead is the durable-path negative
// control matching TestPrepareAbortsWhenRefreshFindsAStaleRead: a genuinely stale read
// must still abort, not be papered over by refresh, even against a real Raft group.
func TestDurableStorePrepareAbortsWhenRefreshFindsAStaleRead(t *testing.T) {
	rng := startDurableRange(t, 2)
	store := NewDurableStore(rng)

	seedAndConfirm := func(key, value string) {
		t.Helper()
		if err := rng.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if got, err := rng.Get([]byte(key)); err == nil && string(got) == value {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("seed %s never became visible", key)
	}
	seedAndConfirm("a", "on-call")
	seedAndConfirm("b", "on-call")

	t1, t2 := Timestamp{WallTime: 100}, Timestamp{WallTime: 200}
	tMid := Timestamp{WallTime: 150}

	if v, err := rng.Get([]byte("b")); err != nil || string(v) != "on-call" {
		t.Fatalf("T1 read b = %q, %v", v, err)
	}
	if err := store.RecordRead([]byte("a"), t2); err != nil {
		t.Fatalf("RecordRead: %v", err)
	}

	// Some other, already-committed transaction wrote "b" at tMid -- strictly after T1's
	// own read of it and strictly before the timestamp T1 would be pushed to.
	if err := store.WriteIntent(Intent{Key: []byte("b"), Value: []byte("off-call"), TxnID: "other", Timestamp: tMid}); err != nil {
		t.Fatalf("setup write to b failed: %v", err)
	}
	if err := store.Resolve(Record{ID: "other", Status: Committed, WriteTimestamp: tMid}); err != nil {
		t.Fatalf("setup resolve failed: %v", err)
	}

	clock := NewClock(func() time.Time { return time.Unix(0, t1.WallTime) })
	_, err := NewCoordinator(clock).Prepare("t1", []WriteSet{{
		Store:   store,
		Intents: []Intent{{Key: []byte("a"), Value: []byte("off-call")}},
		Reads:   [][]byte{[]byte("b")},
	}})
	if err == nil {
		t.Fatal("Prepare committed a transaction whose own read was invalidated by an intervening write over real Raft replication -- refresh should have failed")
	}
}
