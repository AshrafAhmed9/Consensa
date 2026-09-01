package txn

import "errors"

// Participant is what the coordinator needs from a transaction range -- record storage
// plus provisional intent installation and resolution. *Store (in-memory, for tests and
// the coordinator's own unit tests) and *DurableStore (backed by a real Raft-replicated
// kv.DurableRange) both satisfy it: the 2PC protocol logic in this file is identical
// either way, and only what "durable" means underneath changes.
type Participant interface {
	PutRecord(Record) error
	Record(id string) (Record, bool)
	WriteIntent(Intent) error
	Resolve(Record) error
}

// Coordinator drives a minimal two-phase commit across participant stores.
type Coordinator struct{ clock *Clock }

// NewCoordinator creates a coordinator using an HLC for transaction timestamps.
func NewCoordinator(clock *Clock) *Coordinator { return &Coordinator{clock: clock} }

// WriteSet names one participant and the intents it must prepare. A slice, rather than a
// map, deliberately preserves the first writer as the transaction-record anchor; this
// mirrors the range selection rule without depending on Go's randomized map iteration.
type WriteSet struct {
	Store   Participant
	Intents []Intent
}

// Transaction is the coordinator's recoverable description of a prepared transaction.
// Anchor is the sole authority for the commit decision; participant copies are only intent
// cleanup state and can therefore lag safely after a coordinator crash.
type Transaction struct {
	Record       Record
	Anchor       Participant
	Participants []Participant
}

// Prepare writes a pending record to the first participant and installs all provisional
// intents. It never makes a value visible, so a failure can safely publish Aborted and
// clean up every participant that was reached.
func (c *Coordinator) Prepare(id string, writes []WriteSet) (*Transaction, error) {
	if id == "" || len(writes) == 0 || writes[0].Store == nil {
		return nil, errors.New("txn: ID and first participant required")
	}
	ts := c.clock.Now()
	txn := &Transaction{
		Record:       Record{ID: id, ReadTimestamp: ts, WriteTimestamp: ts, Status: Pending},
		Anchor:       writes[0].Store,
		Participants: make([]Participant, 0, len(writes)),
	}
	if err := txn.Anchor.PutRecord(txn.Record); err != nil {
		return nil, err
	}
	for _, write := range writes {
		if write.Store == nil {
			txn.Record.Status = Aborted
			_ = txn.Anchor.PutRecord(txn.Record)
			_ = c.Resolve(txn)
			return nil, errors.New("txn: nil participant")
		}
		txn.Participants = append(txn.Participants, write.Store)
		for _, intent := range write.Intents {
			intent.TxnID = id
			intent.Timestamp = ts
			if err := write.Store.WriteIntent(intent); err != nil {
				txn.Record.Status = Aborted
				_ = txn.Anchor.PutRecord(txn.Record)
				_ = c.Resolve(txn)
				return nil, err
			}
		}
	}
	return txn, nil
}

// CommitRecord performs the transaction's atomic commit point. In the final range-backed
// implementation this is one Raft command on Anchor; intent resolution remains separate,
// so a crash after this method cannot lose the committed decision.
func (c *Coordinator) CommitRecord(txn *Transaction) error {
	if txn == nil || txn.Anchor == nil {
		return errors.New("txn: missing transaction anchor")
	}
	record, ok := txn.Anchor.Record(txn.Record.ID)
	if !ok || record.Status != Pending {
		return errors.New("txn: transaction is not pending")
	}
	record.Status = Committed
	if err := txn.Anchor.PutRecord(record); err != nil {
		return err
	}
	txn.Record = record
	return nil
}

// Resolve applies the anchor's final decision to every reached participant. It is safe to
// call repeatedly: resolved intents are absent on later attempts, which is the key property
// a restarted coordinator needs.
func (c *Coordinator) Resolve(txn *Transaction) error {
	if txn == nil || txn.Anchor == nil {
		return errors.New("txn: missing transaction anchor")
	}
	record, ok := txn.Anchor.Record(txn.Record.ID)
	if !ok || record.Status == Pending {
		return errors.New("txn: transaction has no final record")
	}
	var firstErr error
	for _, participant := range txn.Participants {
		// Resolve is called for every participant even after one fails: it's safe to
		// call repeatedly (Store.Resolve and DurableStore.Resolve are both idempotent),
		// so a transient failure on one range must not stop cleanup on the others -- a
		// later retry (or a restarted coordinator, per this method's own doc comment)
		// picks up wherever this pass left off.
		if err := participant.Resolve(record); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	txn.Record = record
	return firstErr
}

// Commit writes all intents then resolves them committed. A preparation failure aborts all
// participants that accepted an intent, preserving atomic visibility in this model.
func (c *Coordinator) Commit(id string, writes map[Participant][]Intent) error {
	sets := make([]WriteSet, 0, len(writes))
	for store, intents := range writes {
		sets = append(sets, WriteSet{Store: store, Intents: intents})
	}
	txn, err := c.Prepare(id, sets)
	if err != nil {
		return err
	}
	if err := c.CommitRecord(txn); err != nil {
		return err
	}
	return c.Resolve(txn)
}
