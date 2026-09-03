package txn

import (
	"errors"

	"github.com/ashraf/consensa/internal/metrics"
)

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
	// PushedWriteTimestamp and RefreshReads implement the read-refresh path Prepare falls
	// back to on ErrWriteBelowObservedRead (see Prepare's own doc comment) -- both pure
	// queries, never durable writes, so a participant that cannot support refresh yet can
	// implement them as PushedWriteTimestamp returning ts unchanged and RefreshReads
	// always returning false, which degrades safely back to today's abort-and-retry
	// behavior rather than a compile-time requirement every Participant must satisfy.
	PushedWriteTimestamp(key []byte, ts Timestamp) Timestamp
	RefreshReads(reads map[string]Timestamp, newTS Timestamp) bool
}

// Coordinator drives a minimal two-phase commit across participant stores.
type Coordinator struct {
	clock   *Clock
	metrics *metrics.Registry
}

// NewCoordinator creates a coordinator using an HLC for transaction timestamps.
func NewCoordinator(clock *Clock) *Coordinator { return &Coordinator{clock: clock} }

// SetMetrics attaches the process-wide metrics registry so Commit can record outcomes.
// Optional and separate from NewCoordinator, the same way server.Service.SetMetrics is,
// so every existing call site -- production and the package's own unit tests -- keeps
// working unmodified; a Coordinator with no attached registry just skips the Inc call.
func (c *Coordinator) SetMetrics(m *metrics.Registry) { c.metrics = m }

// WriteSet names one participant and the intents it must prepare. A slice, rather than a
// map, deliberately preserves the first writer as the transaction-record anchor; this
// mirrors the range selection rule without depending on Go's randomized map iteration.
type WriteSet struct {
	Store   Participant
	Intents []Intent
	// Reads names keys this participant's caller already read (via Store.RecordRead) at
	// this same transaction's read timestamp. It is what makes read-refresh possible on
	// an ErrWriteBelowObservedRead conflict (see Prepare's doc comment) -- without it,
	// Prepare has no record of what this transaction itself has read and must always fall
	// back to abort-and-retry. Optional: a WriteSet with no reads to protect just leaves
	// this nil and refresh degrades to "nothing to invalidate," never blocking it.
	Reads [][]byte
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
//
// On an ErrWriteBelowObservedRead conflict, Prepare attempts one read-refresh (the piece
// docs/notes/14-serializable.md named as still missing) before falling back to abort: it
// computes how far the whole transaction's timestamp would need to move to clear every
// conflicting key at once (maxPushedTimestamp), asks every participant whether this
// transaction's own prior reads are still valid at that later timestamp (refreshAll), and
// if so retries every intent at the new timestamp instead of aborting and forcing the
// caller to retry the whole transaction from scratch. This is deliberately a single
// attempt, not a loop: a second conflict discovered during the retry itself (another
// transaction committed in the narrow window between computing the push and re-installing)
// falls back to abort like any other unresolvable conflict, rather than retrying
// indefinitely against a workload that may never let it succeed.
func (c *Coordinator) Prepare(id string, writes []WriteSet) (*Transaction, error) {
	if id == "" || len(writes) == 0 || writes[0].Store == nil {
		return nil, errors.New("txn: ID and first participant required")
	}
	for _, write := range writes {
		if write.Store == nil {
			return nil, errors.New("txn: nil participant")
		}
	}
	ts := c.clock.Now()
	txn := &Transaction{
		Record:       Record{ID: id, ReadTimestamp: ts, WriteTimestamp: ts, Status: Pending},
		Anchor:       writes[0].Store,
		Participants: make([]Participant, 0, len(writes)),
	}
	for _, write := range writes {
		txn.Participants = append(txn.Participants, write.Store)
	}
	if err := txn.Anchor.PutRecord(txn.Record); err != nil {
		return nil, err
	}
	if err := c.installIntents(id, writes, ts); err != nil {
		if !errors.Is(err, ErrWriteBelowObservedRead) {
			txn.Record.Status = Aborted
			_ = txn.Anchor.PutRecord(txn.Record)
			_ = c.Resolve(txn)
			return nil, err
		}
		pushed := c.maxPushedTimestamp(writes, ts)
		if !c.refreshAll(writes, ts, pushed) {
			txn.Record.Status = Aborted
			_ = txn.Anchor.PutRecord(txn.Record)
			_ = c.Resolve(txn)
			return nil, err
		}
		if err := c.installIntents(id, writes, pushed); err != nil {
			txn.Record.Status = Aborted
			_ = txn.Anchor.PutRecord(txn.Record)
			_ = c.Resolve(txn)
			return nil, err
		}
		ts = pushed
		txn.Record.ReadTimestamp = ts
		txn.Record.WriteTimestamp = ts
		if err := txn.Anchor.PutRecord(txn.Record); err != nil {
			return nil, err
		}
	}
	return txn, nil
}

// installIntents proposes every write set's intents at ts. It is called twice by a
// refreshed Prepare (once at the original timestamp, once at the pushed one) -- safe
// because WriteIntent allows a participant's own transaction ID to overwrite its own
// prior intent on the same key (see WriteIntent's own doc comment), so re-proposing an
// intent that already installed cleanly at the old timestamp is a harmless no-op write,
// not a conflict.
func (c *Coordinator) installIntents(id string, writes []WriteSet, ts Timestamp) error {
	for _, write := range writes {
		for _, intent := range write.Intents {
			intent.TxnID = id
			intent.Timestamp = ts
			if err := write.Store.WriteIntent(intent); err != nil {
				return err
			}
		}
	}
	return nil
}

// maxPushedTimestamp asks every participant what timestamp each of its intents would need
// to clear WriteIntent's observed-read check, and returns the latest of them -- the single
// timestamp a refreshed retry must use so every intent across every participant clears at
// once, not just the one that happened to conflict first.
func (c *Coordinator) maxPushedTimestamp(writes []WriteSet, ts Timestamp) Timestamp {
	max := ts
	for _, write := range writes {
		for _, intent := range write.Intents {
			if pushed := write.Store.PushedWriteTimestamp(intent.Key, ts); pushed.Compare(max) > 0 {
				max = pushed
			}
		}
	}
	return max
}

// refreshAll asks every participant to validate its share of this transaction's read set
// (WriteSet.Reads) against the pushed timestamp, using the original read timestamp as the
// floor below which a conflicting write would have already been visible to this
// transaction anyway. Any participant reporting even one stale read fails the whole
// refresh -- serializability needs every read this transaction relied on to still be
// current at the timestamp it is about to commit at, not just a majority of them.
func (c *Coordinator) refreshAll(writes []WriteSet, originalTS, pushed Timestamp) bool {
	for _, write := range writes {
		if len(write.Reads) == 0 {
			continue
		}
		reads := make(map[string]Timestamp, len(write.Reads))
		for _, key := range write.Reads {
			reads[string(key)] = originalTS
		}
		if !write.Store.RefreshReads(reads, pushed) {
			return false
		}
	}
	return true
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
func (c *Coordinator) Commit(id string, writes map[Participant][]Intent) (err error) {
	if c.metrics != nil {
		defer func() {
			outcome := "success"
			if err != nil {
				outcome = "failure"
			}
			c.metrics.TxnCommits.WithLabelValues(outcome).Inc()
		}()
	}
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
