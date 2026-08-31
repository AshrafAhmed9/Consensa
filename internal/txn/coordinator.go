package txn

import "errors"

// Coordinator drives a minimal two-phase commit across participant stores.
type Coordinator struct{ clock *Clock }

// NewCoordinator creates a coordinator using an HLC for transaction timestamps.
func NewCoordinator(clock *Clock) *Coordinator { return &Coordinator{clock: clock} }

// WriteSet names one participant and the intents it must prepare. A slice, rather than a
// map, deliberately preserves the first writer as the transaction-record anchor; this
// mirrors the range selection rule without depending on Go's randomized map iteration.
type WriteSet struct {
	Store   *Store
	Intents []Intent
}

// Transaction is the coordinator's recoverable description of a prepared transaction.
// Anchor is the sole authority for the commit decision; participant copies are only intent
// cleanup state and can therefore lag safely after a coordinator crash.
type Transaction struct {
	Record       Record
	Anchor       *Store
	Participants []*Store
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
		Participants: make([]*Store, 0, len(writes)),
	}
	txn.Anchor.PutRecord(txn.Record)
	for _, write := range writes {
		if write.Store == nil {
			txn.Record.Status = Aborted
			txn.Anchor.PutRecord(txn.Record)
			c.Resolve(txn)
			return nil, errors.New("txn: nil participant")
		}
		txn.Participants = append(txn.Participants, write.Store)
		for _, intent := range write.Intents {
			intent.TxnID = id
			intent.Timestamp = ts
			if err := write.Store.WriteIntent(intent); err != nil {
				txn.Record.Status = Aborted
				txn.Anchor.PutRecord(txn.Record)
				c.Resolve(txn)
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
	txn.Anchor.PutRecord(record)
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
	for _, participant := range txn.Participants {
		participant.Resolve(record)
	}
	txn.Record = record
	return nil
}

// Commit writes all intents then resolves them committed. A preparation failure aborts all
// participants that accepted an intent, preserving atomic visibility in this model.
func (c *Coordinator) Commit(id string, writes map[*Store][]Intent) error {
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
