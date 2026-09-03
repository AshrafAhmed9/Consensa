package txn

import (
	"errors"
	"testing"
	"time"
)

// TestReadAtTimestampRestartsUnderClockSkew reproduces the actual uncertainty-interval
// anomaly docs/notes/14-serializable.md named as future work, using two simulated clocks
// with a known, fixed skew between them -- not just an isolated timestamp-arithmetic check.
//
// Node F ("fast") and node S ("slow") each run their own *Clock, skewed by exactly
// skew = 50ms: F's wall clock always reads 50ms ahead of S's. A write commits on F at
// F's Now() (real time T0+50ms by F's clock). A read then starts on S at S's Now() (real
// time T0 by S's clock) -- in real wall-clock time the read started AFTER the write
// committed (both fired at the same instant T0 real time in this test, S's clock is just
// slower), so a correct system must not let the read silently miss the write. Because F's
// clock could legitimately be up to MaxOffset ahead of S's, S's read at ts cannot rule out
// that a value timestamped anywhere in (ts, ts+MaxOffset] actually happened-before it in
// real time; ReadAtTimestamp must therefore refuse to answer (ErrUncertainRead) rather than
// return the write's value in a way that could imply the read used it as its "read below
// the write" snapshot -- or silently return nothing at all when the value is exactly the
// one this read must not miss. The caller retries at UncertaintyRestartTimestamp, past the
// uncertain value, and only that second read succeeds.
func TestReadAtTimestampRestartsUnderClockSkew(t *testing.T) {
	const skew = 50 * time.Millisecond
	const maxOffset = 100 * time.Millisecond // cluster-configured bound skew must stay under

	base := time.Unix(1_700_000_000, 0)
	fastClock := NewClock(func() time.Time { return base.Add(skew) }) // F's clock: 50ms fast
	slowClock := NewClock(func() time.Time { return base })           // S's clock: baseline

	store := NewStore()
	store.SetMaxOffset(maxOffset)
	store.values["on-call"] = []byte("dr-a")

	// A write commits on the fast node's clock.
	writeTS := fastClock.Now()
	if err := store.WriteIntent(Intent{Key: []byte("on-call"), Value: []byte("dr-b"), TxnID: "fast-writer", Timestamp: writeTS}); err != nil {
		t.Fatalf("write on fast clock: %v", err)
	}
	if err := store.Resolve(Record{ID: "fast-writer", Status: Committed, WriteTimestamp: writeTS}); err != nil {
		t.Fatalf("resolve fast write: %v", err)
	}

	// A read starts on the slow node's clock, at real time <= the write's real commit time
	// (S's clock reads baseline, 50ms behind F's) but whose HLC timestamp is still within
	// maxOffset of the write's -- exactly the case a slow-but-in-bounds clock produces.
	readTS := slowClock.Now()
	if readTS.Compare(writeTS) >= 0 {
		t.Fatalf("test setup: read timestamp %v must be behind write timestamp %v to exercise skew", readTS, writeTS)
	}

	_, err := store.ReadAtTimestamp([]byte("on-call"), readTS)
	if !errors.Is(err, ErrUncertainRead) {
		t.Fatalf("ReadAtTimestamp at %v = %v, want ErrUncertainRead -- a write at %v inside the %v uncertainty window was not detected",
			readTS, err, writeTS, maxOffset)
	}

	// Retrying at the bumped timestamp must succeed and see the write.
	restartTS := store.UncertaintyRestartTimestamp([]byte("on-call"))
	if restartTS.Compare(writeTS) <= 0 {
		t.Fatalf("restart timestamp %v was not pushed past the uncertain write %v", restartTS, writeTS)
	}
	v, err := store.ReadAtTimestamp([]byte("on-call"), restartTS)
	if err != nil {
		t.Fatalf("ReadAtTimestamp at restart timestamp %v: %v", restartTS, err)
	}
	if string(v) != "dr-b" {
		t.Fatalf("restarted read = %q, want the fast node's committed write %q", v, "dr-b")
	}
}

// TestReadAtTimestampOutsideWindowDoesNotRestart is the negative control: a write whose
// timestamp is beyond maxOffset from the read (a skew larger than any node is configured to
// tolerate would be a real clock fault, out of scope here) must not force a restart, and a
// write safely BEFORE the read's own timestamp is not uncertain at all -- this mechanism
// must not turn every read into a restart.
func TestReadAtTimestampOutsideWindowDoesNotRestart(t *testing.T) {
	const maxOffset = 100 * time.Millisecond
	store := NewStore()
	store.SetMaxOffset(maxOffset)
	store.values["k"] = []byte("v0")

	readTS := Timestamp{WallTime: 1000}

	// A write safely before the read: not uncertain.
	before := Timestamp{WallTime: 900}
	if err := store.WriteIntent(Intent{Key: []byte("k"), Value: []byte("v-before"), TxnID: "t-before", Timestamp: before}); err != nil {
		t.Fatalf("write before: %v", err)
	}
	if err := store.Resolve(Record{ID: "t-before", Status: Committed, WriteTimestamp: before}); err != nil {
		t.Fatalf("resolve before: %v", err)
	}
	if _, err := store.ReadAtTimestamp([]byte("k"), readTS); err != nil {
		t.Fatalf("read of a write strictly before ts incorrectly treated as uncertain: %v", err)
	}

	// A write far beyond the uncertainty window: also not uncertain (out of range).
	store2 := NewStore()
	store2.SetMaxOffset(maxOffset)
	store2.values["k"] = []byte("v0")
	farFuture := Timestamp{WallTime: readTS.WallTime + int64(maxOffset) + int64(time.Second)}
	if err := store2.WriteIntent(Intent{Key: []byte("k"), Value: []byte("v-far"), TxnID: "t-far", Timestamp: farFuture}); err != nil {
		t.Fatalf("write far future: %v", err)
	}
	if err := store2.Resolve(Record{ID: "t-far", Status: Committed, WriteTimestamp: farFuture}); err != nil {
		t.Fatalf("resolve far future: %v", err)
	}
	if _, err := store2.ReadAtTimestamp([]byte("k"), readTS); err != nil {
		t.Fatalf("read with only a far-future write present incorrectly treated as uncertain: %v", err)
	}

	// maxOffset disabled (zero, NewStore's default) never restarts, even for a value
	// squarely inside what would otherwise be the uncertainty window.
	store3 := NewStore()
	store3.values["k"] = []byte("v0")
	inWindow := Timestamp{WallTime: readTS.WallTime + int64(maxOffset) / 2}
	if err := store3.WriteIntent(Intent{Key: []byte("k"), Value: []byte("v-in"), TxnID: "t-in", Timestamp: inWindow}); err != nil {
		t.Fatalf("write in-window: %v", err)
	}
	if err := store3.Resolve(Record{ID: "t-in", Status: Committed, WriteTimestamp: inWindow}); err != nil {
		t.Fatalf("resolve in-window: %v", err)
	}
	if _, err := store3.ReadAtTimestamp([]byte("k"), readTS); err != nil {
		t.Fatalf("uncertainty checking fired despite maxOffset being disabled (zero value): %v", err)
	}
}

// TestDurableStoreReadAtTimestampRestartsUnderClockSkew is TestReadAtTimestampRestartsUnderClockSkew
// against a real 3-node kv.DurableRange group instead of the in-memory Store, matching this
// file's own established real-Raft-group testing convention (see e.g.
// TestDurableStoreRejectsWriteSkew, durable_store_test.go).
func TestDurableStoreReadAtTimestampRestartsUnderClockSkew(t *testing.T) {
	rng := startDurableRange(t, 3)
	store := NewDurableStore(rng)
	const maxOffset = 100 * time.Millisecond
	store.SetMaxOffset(maxOffset)

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
	seedAndConfirm("on-call", "dr-a")

	const skew = 50 * time.Millisecond
	base := time.Unix(1_700_000_000, 0)
	fastClock := NewClock(func() time.Time { return base.Add(skew) })
	slowClock := NewClock(func() time.Time { return base })

	writeTS := fastClock.Now()
	if err := store.WriteIntent(Intent{Key: []byte("on-call"), Value: []byte("dr-b"), TxnID: "fast-writer", Timestamp: writeTS}); err != nil {
		t.Fatalf("write on fast clock: %v", err)
	}
	if err := store.Resolve(Record{ID: "fast-writer", Status: Committed, WriteTimestamp: writeTS}); err != nil {
		t.Fatalf("resolve fast write: %v", err)
	}

	readTS := slowClock.Now()
	if readTS.Compare(writeTS) >= 0 {
		t.Fatalf("test setup: read timestamp %v must be behind write timestamp %v to exercise skew", readTS, writeTS)
	}

	_, err := store.ReadAtTimestamp([]byte("on-call"), readTS)
	if !errors.Is(err, ErrUncertainRead) {
		t.Fatalf("ReadAtTimestamp at %v = %v, want ErrUncertainRead over real Raft replication", readTS, err)
	}

	restartTS := store.UncertaintyRestartTimestamp([]byte("on-call"))
	if restartTS.Compare(writeTS) <= 0 {
		t.Fatalf("restart timestamp %v was not pushed past the uncertain write %v", restartTS, writeTS)
	}
	v, err := store.ReadAtTimestamp([]byte("on-call"), restartTS)
	if err != nil {
		t.Fatalf("ReadAtTimestamp at restart timestamp %v: %v", restartTS, err)
	}
	if string(v) != "dr-b" {
		t.Fatalf("restarted read = %q, want the fast node's committed write %q", v, "dr-b")
	}
}
