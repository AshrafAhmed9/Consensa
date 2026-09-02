package txn

import (
	"testing"
	"time"
)

// TestPrepareRefreshesInsteadOfAbortingWhenPriorReadsStillHold proves the read-refresh
// path docs/notes/14-serializable.md named as still missing: T1 starts before T2 but its
// Prepare call for key "a" arrives after T2 has already recorded a later read of "a" (the
// same ErrWriteBelowObservedRead conflict TestWriteIntentRejectsWriteSkew reproduces).
// Unlike that test, T1 here also has its OWN prior read of an unrelated key "b" that
// nothing has since written -- so refresh should succeed, and Prepare must commit T1 at a
// pushed timestamp instead of aborting it outright.
func TestPrepareRefreshesInsteadOfAbortingWhenPriorReadsStillHold(t *testing.T) {
	store := NewStore()
	store.values["a"] = []byte("on-call")
	store.values["b"] = []byte("on-call")

	t1 := Timestamp{WallTime: 100}
	t2 := Timestamp{WallTime: 200}

	// T1 reads "b" at t1 -- this is the read Prepare's refresh must re-validate.
	if v, err := store.Get([]byte("b")); err != nil || string(v) != "on-call" {
		t.Fatalf("T1 read b = %q, %v", v, err)
	}

	// T2 reads "a" at t2, later than t1 -- the conflict T1's write to "a" will hit.
	store.RecordRead([]byte("a"), t2)

	clock := NewClock(func() time.Time { return time.Unix(0, t1.WallTime) })
	txn, err := NewCoordinator(clock).Prepare("t1", []WriteSet{{
		Store:   store,
		Intents: []Intent{{Key: []byte("a"), Value: []byte("off-call")}},
		Reads:   [][]byte{[]byte("b")},
	}})
	if err != nil {
		t.Fatalf("Prepare should have refreshed and committed instead of aborting: %v", err)
	}
	if txn.Record.WriteTimestamp.Compare(t2) <= 0 {
		t.Fatalf("write timestamp %v was not pushed past the conflicting read %v", txn.Record.WriteTimestamp, t2)
	}

	// The intent must actually be installed at the pushed timestamp, not the original.
	intent, ok := store.intents["a"]
	if !ok {
		t.Fatal("refreshed transaction's intent was never installed")
	}
	if intent.Timestamp.Compare(t2) <= 0 {
		t.Fatalf("installed intent timestamp %v was not pushed past %v", intent.Timestamp, t2)
	}
}

// TestPrepareAbortsWhenRefreshFindsAStaleRead proves refresh does not paper over a
// genuinely stale read: if some other transaction committed a write to a key T1 read
// between T1's read and the pushed timestamp, T1's original read really is invalid at
// the new timestamp and Prepare must still abort, not silently commit an unsafe schedule.
func TestPrepareAbortsWhenRefreshFindsAStaleRead(t *testing.T) {
	store := NewStore()
	store.values["a"] = []byte("on-call")
	store.values["b"] = []byte("on-call")

	t1 := Timestamp{WallTime: 100}
	t2 := Timestamp{WallTime: 200}
	tMid := Timestamp{WallTime: 150}

	if v, err := store.Get([]byte("b")); err != nil || string(v) != "on-call" {
		t.Fatalf("T1 read b = %q, %v", v, err)
	}
	store.RecordRead([]byte("a"), t2)

	// Some other, already-committed transaction wrote "b" at tMid -- strictly after T1's
	// own read of it (t1) and strictly before the timestamp T1 would be pushed to (> t2).
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
		t.Fatal("Prepare committed a transaction whose own read was invalidated by an intervening write -- refresh should have failed")
	}
}
