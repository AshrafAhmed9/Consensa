package txn

import "errors"

// Status is the sole authority for resolving a provisional write.
type Status uint8

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
}

// NewStore creates an empty transaction participant.
func NewStore() *Store {
	return &Store{records: map[string]Record{}, intents: map[string]Intent{}, values: map[string][]byte{}}
}

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
func (s *Store) WriteIntent(intent Intent) error {
	if existing, ok := s.intents[string(intent.Key)]; ok && existing.TxnID != intent.TxnID {
		return errors.New("txn: write intent conflict")
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
		}
		delete(s.intents, key)
	}
	return nil
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
