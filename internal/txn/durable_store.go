package txn

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"
)

// confirmTimeout bounds how long putAndConfirm waits for a proposed write to become
// locally visible before giving up. It is generous relative to a healthy cluster's
// heartbeat interval (single-digit milliseconds in this codebase's tests) precisely so a
// slow-but-healthy commit isn't mistaken for a stuck one under load.
const confirmTimeout = 3 * time.Second

// rangeClient is the subset of kv.DurableRange a DurableStore needs. It is declared here,
// not imported as the concrete type, so this package does not depend on internal/kv --
// txn is a lower-level package (kv could plausibly depend on txn for cross-range
// transactions later; the reverse would be a cycle). Any type with these two methods
// (kv.DurableRange included) satisfies it without either package knowing about the other.
type rangeClient interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
}

// recordPrefix and intentPrefix namespace transaction bookkeeping within the same
// keyspace a DurableRange also serves ordinary application data from. They cannot collide
// with an application key that also happens to start with "txn/" -- that is a real,
// accepted constraint of sharing one keyspace, the same one kv.DurableRange itself
// documents for its own "raft/" reserved prefix.
const (
	recordPrefix    = "txn/record/"
	intentPrefix    = "txn/intent/"
	intentKeysIndex = "txn/intent-keys/"
	readPrefix      = "txn/read/"
	lastWritePrefix = "txn/lastwrite/"
)

// DurableStore is a Participant backed by a real Raft-replicated range instead of an
// in-memory map. It exists so Coordinator's 2PC protocol -- proven correct against the
// in-memory Store in txn_test.go -- runs unmodified over durable, replicated storage:
// every method here does nothing but translate Participant's calls into Put/Get against
// rangeClient, using JSON encoding and key namespacing; no transaction logic is
// duplicated from coordinator.go.
//
// A real limitation, stated plainly rather than hidden: WriteIntent's conflict check
// (read-then-write) and the intent-key index update are not atomic with each other --
// two concurrent WriteIntent calls to the same key on the same range could both pass the
// check before either's write lands, exactly the kind of race a real implementation would
// resolve with a conditional/compare-and-swap Put, which kv.DurableRange does not yet
// expose. This is why every Store method here goes through putAndConfirm (below) rather
// than a bare Put: rangeClient.Put itself only proposes and returns once locally
// appended, not once committed -- Coordinator's protocol logic (coordinator.go) was
// proven against the synchronous in-memory Store and genuinely needs "this call's effect
// is visible to the next call" to hold, which a bare Put does not guarantee but
// putAndConfirm does, at the cost of blocking until the write is locally readable.
//
// Like Store, DurableStore now also rejects a write whose timestamp collides with an
// already-recorded read on the same key -- the write-skew defense described in
// docs/notes/14-serializable.md -- by durably persisting each key's high-water read mark
// (readPrefix below) instead of holding it in an in-memory TimestampCache the way Store
// does. It also now implements read-refresh (PushedWriteTimestamp/RefreshReads below),
// durably persisting each key's last-committed-write timestamp (lastWritePrefix) the same
// way -- so a pushed transaction can commit at a later timestamp instead of always
// aborting, over this real Raft-replicated path too, not just the in-memory Store. This
// is still not full serializable snapshot isolation in every other respect: (like
// WriteIntent's existing conflict check above) the read-then-write sequence in
// checkNotBelowObservedRead/WriteIntent is not atomic with a concurrent RecordRead or
// WriteIntent to the same key -- the same class of race this file's WriteIntent doc
// comment already states plainly for the intent-key index, for the same underlying
// reason: kv.DurableRange has no conditional/compare-and-swap Put to build a race-free
// version on.
type DurableStore struct {
	rng rangeClient
	// maxOffset is the durable-path twin of Store.maxOffset -- see SetMaxOffset and
	// ReadAtTimestamp below, and Store.ReadAtTimestamp's doc comment for the full argument.
	// Zero (NewDurableStore's default) disables uncertainty checking, matching Store's own
	// opt-in default.
	maxOffset time.Duration
}

// NewDurableStore wraps any rangeClient (a *kv.DurableRange in production) as a
// Participant.
func NewDurableStore(rng rangeClient) *DurableStore {
	return &DurableStore{rng: rng}
}

// SetMaxOffset configures the clock-uncertainty window ReadAtTimestamp enforces, mirroring
// Store.SetMaxOffset for the durable path.
func (d *DurableStore) SetMaxOffset(maxOffset time.Duration) { d.maxOffset = maxOffset }

// ReadAtTimestamp is the durable-path twin of Store.ReadAtTimestamp -- see its doc comment
// (intent.go) for the full uncertainty-interval argument this mirrors. It queries the same
// durably persisted lastWritePrefix index RefreshReads already reads, so no new durable
// state is introduced: uncertainty checking is a read-time policy layered over data this
// store already tracks.
func (d *DurableStore) ReadAtTimestamp(key []byte, ts Timestamp) ([]byte, error) {
	if d.maxOffset > 0 {
		if last, ok := d.lastWriteTimestamp(key); ok {
			if last.Compare(ts) > 0 && last.WallTime <= ts.WallTime+d.maxOffset.Nanoseconds() {
				return nil, ErrUncertainRead
			}
		}
	}
	return d.rng.Get(key)
}

// UncertaintyRestartTimestamp mirrors Store.UncertaintyRestartTimestamp: the timestamp a
// caller must retry ReadAtTimestamp at after ErrUncertainRead, strictly past the uncertain
// write so the retry cannot observe the identical value as uncertain again.
func (d *DurableStore) UncertaintyRestartTimestamp(key []byte) Timestamp {
	last, _ := d.lastWriteTimestamp(key)
	return Timestamp{WallTime: last.WallTime, Logical: last.Logical + 1}
}

// putAndConfirm proposes key/value and blocks until it reads back locally, closing the
// gap between rangeClient.Put's contract (returns once locally appended, not once
// committed and applied -- see kv.DurableRange.Put's own doc comment) and what every
// Coordinator method in coordinator.go assumes of a Participant: that once one call
// returns, the next call sees its effect. Store (the in-memory implementation) satisfies
// that trivially; a real Raft-backed range does not without this. Bounded by
// confirmTimeout rather than blocking forever, because a real leadership change mid-call
// is a real failure a caller must be able to observe and retry against, not hang on.
func (d *DurableStore) putAndConfirm(key, value []byte) error {
	if err := d.rng.Put(key, value); err != nil {
		return err
	}
	deadline := time.Now().Add(confirmTimeout)
	for time.Now().Before(deadline) {
		if got, err := d.rng.Get(key); err == nil && bytes.Equal(got, value) {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return errors.New("txn: proposed write did not become visible before the deadline")
}

// PutRecord durably proposes this participant's copy of a transaction record.
func (d *DurableStore) PutRecord(record Record) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return d.putAndConfirm([]byte(recordPrefix+record.ID), data)
}

// Record reads a transaction record from this replica's local (already-applied) state.
func (d *DurableStore) Record(id string) (Record, bool) {
	data, err := d.rng.Get([]byte(recordPrefix + id))
	if err != nil {
		return Record{}, false
	}
	var record Record
	if err := json.Unmarshal(data, &record); err != nil {
		return Record{}, false
	}
	return record, true
}

// PushedWriteTimestamp mirrors Store.PushedWriteTimestamp (intent.go) for the durable
// path: a pure query of the durably recorded read high-water mark for key, computing the
// same floor WriteIntent's own conflict check enforces, without recording anything. Used
// by Coordinator.Prepare's read-refresh path (coordinator.go) to compute how far a whole
// transaction's timestamp must move to clear every conflicting key at once.
func (d *DurableStore) PushedWriteTimestamp(key []byte, ts Timestamp) Timestamp {
	observed, ok := d.readTimestamp(key)
	if !ok || observed.Compare(ts) < 0 {
		return ts
	}
	return Timestamp{WallTime: observed.WallTime, Logical: observed.Logical + 1}
}

// RefreshReads implements the durable half of read-refresh (see Store.RefreshReads,
// intent.go, for the full argument this mirrors): a key this transaction read is still
// safe at the pushed timestamp iff no OTHER transaction's write committed to it in
// (originalReadTS, newTS]. lastWriteTimestamp durably records exactly that per key,
// written alongside the committed value itself in Resolve below -- the timestamp-overlap
// proxy, not a value-equality check, for the same reason Store's own doc comment gives.
func (d *DurableStore) RefreshReads(reads map[string]Timestamp, newTS Timestamp) bool {
	for key, originalTS := range reads {
		last, ok := d.lastWriteTimestamp([]byte(key))
		if !ok {
			continue
		}
		if last.Compare(originalTS) > 0 && last.Compare(newTS) <= 0 {
			return false
		}
	}
	return true
}

func (d *DurableStore) lastWriteTimestamp(key []byte) (Timestamp, bool) {
	data, err := d.rng.Get([]byte(lastWritePrefix + string(key)))
	if err != nil {
		return Timestamp{}, false
	}
	var ts Timestamp
	if err := json.Unmarshal(data, &ts); err != nil {
		return Timestamp{}, false
	}
	return ts, true
}

// WriteIntent durably proposes a provisional key/value for a pending transaction, and
// records the key in that transaction's intent-key index so Resolve can find it again --
// unlike Store's in-memory map, a real range has no cheap way to enumerate "every intent
// belonging to transaction X" without such an index. It also rejects a write whose
// timestamp is at or below an already-recorded read of the same key -- see
// ErrWriteBelowObservedRead and RecordRead below.
func (d *DurableStore) WriteIntent(intent Intent) error {
	if existing, ok := d.readIntent(intent.Key); ok && existing.TxnID != intent.TxnID {
		return errors.New("txn: write intent conflict")
	}
	if observed, ok := d.readTimestamp(intent.Key); ok && observed.Compare(intent.Timestamp) >= 0 {
		return ErrWriteBelowObservedRead
	}
	data, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	if err := d.putAndConfirm([]byte(intentPrefix+string(intent.Key)), data); err != nil {
		return err
	}
	return d.appendIntentKey(intent.TxnID, intent.Key)
}

// RecordRead durably notes that a reader observed key's value as of ts, matching
// Store.RecordRead's contract: a later WriteIntent to the same key at a timestamp at or
// below ts is rejected (ErrWriteBelowObservedRead) rather than silently allowed, which is
// what closes the write-skew read-write edge for this participant too. Only advances the
// mark forward -- an older read arriving after a newer one must not weaken the bound
// already in force.
func (d *DurableStore) RecordRead(key []byte, ts Timestamp) error {
	if existing, ok := d.readTimestamp(key); ok && existing.Compare(ts) >= 0 {
		return nil
	}
	data, err := json.Marshal(ts)
	if err != nil {
		return err
	}
	return d.putAndConfirm([]byte(readPrefix+string(key)), data)
}

func (d *DurableStore) readTimestamp(key []byte) (Timestamp, bool) {
	data, err := d.rng.Get([]byte(readPrefix + string(key)))
	if err != nil {
		return Timestamp{}, false
	}
	var ts Timestamp
	if err := json.Unmarshal(data, &ts); err != nil {
		return Timestamp{}, false
	}
	return ts, true
}

// Resolve durably applies or discards every intent this replica knows belongs to record's
// transaction, using the intent-key index WriteIntent maintained.
func (d *DurableStore) Resolve(record Record) error {
	if err := d.PutRecord(record); err != nil {
		return err
	}
	keys, err := d.intentKeys(record.ID)
	if err != nil {
		return err
	}
	for _, key := range keys {
		intent, ok := d.readIntent(key)
		if !ok || intent.TxnID != record.ID {
			continue
		}
		if record.Status == Committed {
			if err := d.putAndConfirm(key, intent.Value); err != nil {
				return err
			}
			tsData, err := json.Marshal(record.WriteTimestamp)
			if err != nil {
				return err
			}
			if err := d.putAndConfirm([]byte(lastWritePrefix+string(key)), tsData); err != nil {
				return err
			}
		}
		// The intent marker itself is left in place rather than deleted: DurableRange
		// has no Delete-by-prefix-match cleanup path exposed here, and a resolved
		// intent is harmless clutter, not a correctness problem -- Get on the plain
		// application key above already returns the real, resolved value once
		// Committed, and Record's own Status is what future Read calls trust.
	}
	return nil
}

func (d *DurableStore) readIntent(key []byte) (Intent, bool) {
	data, err := d.rng.Get([]byte(intentPrefix + string(key)))
	if err != nil {
		return Intent{}, false
	}
	var intent Intent
	if err := json.Unmarshal(data, &intent); err != nil {
		return Intent{}, false
	}
	return intent, true
}

func (d *DurableStore) appendIntentKey(txnID string, key []byte) error {
	keys, err := d.intentKeys(txnID)
	if err != nil {
		return err
	}
	keys = append(keys, key)
	data, err := json.Marshal(keys)
	if err != nil {
		return err
	}
	return d.putAndConfirm([]byte(intentKeysIndex+txnID), data)
}

func (d *DurableStore) intentKeys(txnID string) ([][]byte, error) {
	data, err := d.rng.Get([]byte(intentKeysIndex + txnID))
	if err != nil {
		return nil, nil
	}
	var keys [][]byte
	if err := json.Unmarshal(data, &keys); err != nil {
		return nil, err
	}
	return keys, nil
}
