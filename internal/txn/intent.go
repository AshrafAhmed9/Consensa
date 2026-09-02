package txn

import "errors"

// Status is the sole authority for resolving a provisional write.
type Status uint8

// Pending, Committed, and Aborted are Status's three possible values: a transaction
// record starts Pending and moves to exactly one of the other two, never back.
const (
	Pending Status = iota
	Committed
	Aborted
)

// Record contains a transaction's durable decision.
type Record struct {
	ID                            string
	ReadTimestamp, WriteTimestamp Timestamp
	Status                        Status
}

// Intent is a provisional key value associated with one transaction record.
type Intent struct {
	Key, Value []byte
	TxnID      string
	Timestamp  Timestamp
}

// Store models one range's intent table. Raft will make each mutation durable and replicated.
type Store struct {
	records map[string]Record
	intents map[string]Intent
	values  map[string][]byte
	reads   *TimestampCache
	// lastWrite records the commit timestamp of the most recent committed write to each
	// key -- the oracle RefreshReads needs to answer "did anyone write this key after I
	// read it?" without keeping full MVCC history, since a single more-recent timestamp
	// is sufficient to prove a read is stale (see RefreshReads's own doc comment).
	lastWrite map[string]Timestamp
}

// NewStore creates an empty transaction participant.
func NewStore() *Store {
	return &Store{
		records: map[string]Record{}, intents: map[string]Intent{}, values: map[string][]byte{},
		reads: NewTimestampCache(), lastWrite: map[string]Timestamp{},
	}
}

// RecordRead notes that a reader observed key's value as of ts. It exists so a caller that
// wants serializable protection for a read (not every read needs it -- see
// docs/notes/14-serializable.md) can register it explicitly before WriteIntent is asked to
// write below that timestamp; ErrWriteBelowObservedRead is what closes the loop.
func (s *Store) RecordRead(key []byte, ts Timestamp) { s.reads.RecordRead(key, ts) }

// ErrWriteBelowObservedRead is returned when WriteIntent's timestamp collides with an
// already-recorded later read on the same key -- the read-write edge snapshot isolation's
// write skew depends on. This package takes the conservative response (reject the write
// outright, forcing the caller to abort and retry at a higher timestamp) rather than the
// more permissive "prove the resulting schedule is still safe" analysis full serializable
// snapshot isolation performs -- see docs/notes/14-serializable.md for why that's a
// deliberate, stated simplification rather than the complete algorithm.
var ErrWriteBelowObservedRead = errors.New("txn: write timestamp below an already-observed read")

// PutRecord writes this participant's local copy of a transaction record. Only the anchor
// is authoritative for a final decision; other copies are retained so resolver progress is
// observable and restart tests can inspect their local state. The error return exists to
// satisfy Participant -- an in-memory map write cannot fail -- so a Raft-backed
// implementation (DurableStore) can report a real proposal failure through the identical
// interface.
func (s *Store) PutRecord(record Record) error {
	s.records[record.ID] = record
	return nil
}

// Record returns a copy of a transaction record, if known.
func (s *Store) Record(id string) (Record, bool) {
	record, ok := s.records[id]
	return record, ok
}

// WriteIntent reserves a key for a pending transaction; two writers cannot silently overwrite.
// It also rejects a write whose timestamp is already behind a recorded read on the same key
// (RecordRead) -- see ErrWriteBelowObservedRead.
func (s *Store) WriteIntent(intent Intent) error {
	if existing, ok := s.intents[string(intent.Key)]; ok && existing.TxnID != intent.TxnID {
		return errors.New("txn: write intent conflict")
	}
	if pushed := s.reads.PushWrite(intent.Key, intent.Timestamp); pushed.Compare(intent.Timestamp) != 0 {
		return ErrWriteBelowObservedRead
	}
	s.intents[string(intent.Key)] = intent
	return nil
}

// Resolve applies or discards all intents matching a transaction's final decision.
func (s *Store) Resolve(record Record) error {
	_ = s.PutRecord(record)
	for key, intent := range s.intents {
		if intent.TxnID != record.ID {
			continue
		}
		if record.Status == Committed {
			s.values[key] = append([]byte(nil), intent.Value...)
			s.lastWrite[key] = record.WriteTimestamp
		}
		delete(s.intents, key)
	}
	return nil
}

// PushedWriteTimestamp reports the earliest timestamp WriteIntent would accept for key,
// without recording anything -- TimestampCache.PushWrite is a pure query over the
// existing read high-water mark, so this is safe to call speculatively. Coordinator.Prepare
// uses it to compute how far a whole transaction's timestamp must move to clear every
// conflicting key at once, before retrying (see Prepare's read-refresh path).
func (s *Store) PushedWriteTimestamp(key []byte, ts Timestamp) Timestamp {
	return s.reads.PushWrite(key, ts)
}

// RefreshReads implements the read-refresh half of full serializable snapshot isolation
// docs/notes/14-serializable.md named as still missing: rather than aborting outright the
// moment a write collides with an observed read (ErrWriteBelowObservedRead), a coordinator
// can push the whole transaction's timestamp forward and ask every participant whether the
// keys it read are still safe to have read at the new, later timestamp. A key is safe iff
// no OTHER transaction committed a write to it in (originalReadTS, newTS] -- if one did,
// this transaction's original read is provably stale at the new timestamp and no amount of
// re-validation can fix that, so refresh must fail and the caller must fall back to a full
// abort-and-retry. This is intentionally the timestamp-overlap check, not a value-equality
// check: real serializable snapshot isolation implementations (CockroachDB included) use
// the same proxy, because two different writes landing on the identical byte value would
// still need to be treated as a conflict to keep the argument correct in general.
func (s *Store) RefreshReads(reads map[string]Timestamp, newTS Timestamp) bool {
	for key, originalTS := range reads {
		last, ok := s.lastWrite[key]
		if !ok {
			continue
		}
		if last.Compare(originalTS) > 0 && last.Compare(newTS) <= 0 {
			return false
		}
	}
	return true
}

// Get reads a committed value; a pending foreign intent is reported rather than ignored.
func (s *Store) Get(key []byte) ([]byte, error) {
	if _, ok := s.intents[string(key)]; ok {
		return nil, errors.New("txn: unresolved intent")
	}
	return s.committed(key)
}

func (s *Store) committed(key []byte) ([]byte, error) {
	v, ok := s.values[string(key)]
	if !ok {
		return nil, errors.New("txn: not found")
	}
	return append([]byte(nil), v...), nil
}

// Read resolves a foreign intent through the authoritative transaction-record lookup.
// Cleanup can lag after commit, but visibility cannot: a committed record makes the intent
// readable immediately, while an aborted record hides it even before its tombstone cleanup.
func (s *Store) Read(key []byte, lookup func(string) (Record, bool)) ([]byte, error) {
	if intent, ok := s.intents[string(key)]; ok {
		record, known := lookup(intent.TxnID)
		if !known || record.Status == Pending {
			return nil, errors.New("txn: unresolved intent")
		}
		if record.Status == Committed {
			return append([]byte(nil), intent.Value...), nil
		}
	}
	return s.committed(key)
}
