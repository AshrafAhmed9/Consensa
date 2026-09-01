package txn

import (
	"errors"
	"testing"
)

// TestTimestampCachePushesConflictingWrite proves a read cannot be followed by an earlier write.
func TestTimestampCachePushesConflictingWrite(t *testing.T) {
	c := NewTimestampCache()
	read := Timestamp{WallTime: 10}
	c.RecordRead([]byte("doctor-b"), read)
	got := c.PushWrite([]byte("doctor-b"), Timestamp{WallTime: 9})
	if got.Compare(read) <= 0 {
		t.Fatalf("write not pushed: %v", got)
	}
}

// TestWriteIntentRejectsWriteSkew reproduces the textbook write-skew anomaly (two on-call
// doctors, an invariant that at least one stays on call, docs/notes/14-serializable.md) and
// proves Store.WriteIntent now refuses the write that would have silently completed it under
// plain snapshot isolation.
//
// T2 (the later transaction) reads doctor A, then intends to take doctor B off call.
// T1 (the earlier transaction) reads doctor B, then intends to take doctor A off call.
// Neither transaction's write touches a key the other transaction wrote -- the anomaly
// depends entirely on the read-write edges (T1 writes A after T2 read A; T2 writes B after
// T1 read B), which is exactly what plain intent-conflict checking (WriteIntent's existing
// same-key/different-TxnID check) cannot see. If both writes were allowed to commit, both
// doctors would end up off call at once, violating the invariant neither transaction
// individually broke.
func TestWriteIntentRejectsWriteSkew(t *testing.T) {
	store := NewStore()
	store.values["doctor-a"] = []byte("on-call")
	store.values["doctor-b"] = []byte("on-call")

	t1, t2 := Timestamp{WallTime: 100}, Timestamp{WallTime: 200}

	// T2 reads doctor A (still on-call) and registers that read.
	if v, err := store.Get([]byte("doctor-a")); err != nil || string(v) != "on-call" {
		t.Fatalf("T2 read doctor-a = %q, %v", v, err)
	}
	store.RecordRead([]byte("doctor-a"), t2)

	// T1 reads doctor B (still on-call, T2 has not written it yet).
	if v, err := store.Get([]byte("doctor-b")); err != nil || string(v) != "on-call" {
		t.Fatalf("T1 read doctor-b = %q, %v", v, err)
	}

	// T2 takes doctor B off call. Nothing has read doctor-b yet at t2, so this succeeds --
	// exactly as it would need to for the anomaly to occur.
	if err := store.WriteIntent(Intent{Key: []byte("doctor-b"), Value: []byte("off-call"), TxnID: "t2", Timestamp: t2}); err != nil {
		t.Fatalf("T2's write to doctor-b unexpectedly rejected: %v", err)
	}

	// T1 tries to take doctor A off call at t1 < t2. T2 already read doctor-a at t2, so
	// this specific write is the one that would complete the write-skew anomaly -- and it
	// must now be rejected, not silently accepted.
	err := store.WriteIntent(Intent{Key: []byte("doctor-a"), Value: []byte("off-call"), TxnID: "t1", Timestamp: t1})
	if !errors.Is(err, ErrWriteBelowObservedRead) {
		t.Fatalf("T1's write to doctor-a = %v, want ErrWriteBelowObservedRead -- write skew was not prevented", err)
	}

	// Control: the same write, at the same timestamp, against a key nobody has recorded a
	// read on, must still succeed -- this mechanism only rejects the specific colliding
	// case, not writes in general.
	if err := store.WriteIntent(Intent{Key: []byte("doctor-c"), Value: []byte("off-call"), TxnID: "t1", Timestamp: t1}); err != nil {
		t.Fatalf("unrelated write incorrectly rejected: %v", err)
	}
}
