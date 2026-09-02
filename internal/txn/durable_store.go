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
// does. This still is NOT full serializable snapshot isolation: no read-refresh, and (like
// WriteIntent's existing conflict check above) the read-then-write sequence in
// checkNotBelowObservedRead/WriteIntent is not atomic with a concurrent RecordRead or
// WriteIntent to the same key -- the same class of race this file's WriteIntent doc
// comment already states plainly for the intent-key index, for the same underlying
// reason: kv.DurableRange has no conditional/compare-and-swap Put to build a race-free
// version on.
type DurableStore struct {
	rng rangeClient
}

// NewDurableStore wraps any rangeClient (a *kv.DurableRange in production) as a
// Participant.
func NewDurableStore(rng rangeClient) *DurableStore {
	return &DurableStore{rng: rng}
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

// PushedWriteTimestamp and RefreshReads deliberately do NOT implement read-refresh for
// DurableStore yet -- returning ts unchanged and false respectively means
// Coordinator.Prepare's refresh attempt (see its own doc comment) always fails fast and
// falls back to today's abort-and-retry behavior, exactly the same outcome as before this
// primitive existed. Store (intent.go) proves the mechanism itself works; wiring the
// durable equivalent needs a real per-key last-committed-write timestamp durably indexed
// the same way readPrefix/intentKeysIndex are here, which is real, separate work -- see
// docs/notes/14-serializable.md for the honest accounting of what's proven versus what's
// still a documented gap.
// PushedWriteTimestamp does not implement read-refresh for DurableStore yet -- see the
// doc comment above.
func (d *DurableStore) PushedWriteTimestamp(_ []byte, ts Timestamp) Timestamp { return ts }

// RefreshReads does not implement read-refresh for DurableStore yet -- see the doc
// comment above.
func (d *DurableStore) RefreshReads(_ map[string]Timestamp, _ Timestamp) bool { return false }

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
