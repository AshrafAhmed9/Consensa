package kv

import (
	"bytes"
	"errors"
	"math"
	"time"

	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/storage"
)

// reservedKeyPrefix is the namespace raft.Persister uses for its own bookkeeping (hard
// state, log entries, snapshots) in the same storage.Engine a DurableRange's data shares.
// A user key starting with this prefix would silently corrupt Raft's own durable state,
// so it is rejected here rather than merely documented as a caller's responsibility.
var reservedKeyPrefix = []byte("raft/")

const readIndexTimeout = 3 * time.Second

// DurableRange is one range replica backed by a real Raft host over real TCP and a real
// on-disk MVCC storage engine -- the durable counterpart to MultiRaft's in-memory
// rangeState (see multiraft.go), whose own doc comment defers exactly this wiring until
// "the live node/transport lifecycle" is available.
//
// It differs from internal/ann.DurableNode's recovery strategy on purpose, not by
// accident: DurableNode recovers by replaying its whole committed Raft log through
// HNSW.ApplyMutation, because an HNSW graph is a search structure that has to be rebuilt
// in memory regardless. A byte KV range has no such requirement -- storage.Engine already
// is a durable, versioned key/value store -- so DurableRange applies each committed
// command as a real Put/Delete against the engine, using the committing entry's own Raft
// index as its MVCC timestamp. Recovery after a restart is therefore just storage.Engine's
// own WAL/SSTable recovery (proven in internal/storage/engine_test.go); DurableRange adds
// no additional recovery logic of its own, which is the point of building it this way.
type DurableRange struct {
	host *raft.Host
	db   *storage.DB
}

// DurableRangeConfig names one replica's identity, group membership, and durable storage.
type DurableRangeConfig struct {
	ID             raft.NodeID
	GroupPeers     []raft.NodeID
	ListenAddress  string
	TransportPeers map[raft.NodeID]string
	StorageDir     string
	ElectionTick   int
	HeartbeatTick  int
}

// NewDurableRange opens (or recovers) the on-disk engine and starts the Raft host with
// Apply wired directly to it. If StorageDir already holds committed range commands from a
// previous run, they are already durable key/value data in the engine -- there is nothing
// to replay, unlike DurableNode's graph.
func NewDurableRange(cfg DurableRangeConfig) (*DurableRange, error) {
	db, err := storage.Open(storage.Options{Dir: cfg.StorageDir, SyncEvery: 1})
	if err != nil {
		return nil, err
	}
	r := &DurableRange{db: db}

	electionTick, heartbeatTick := cfg.ElectionTick, cfg.HeartbeatTick
	if electionTick == 0 {
		electionTick = 10
	}
	if heartbeatTick == 0 {
		heartbeatTick = 2
	}

	host, err := raft.NewHost(raft.HostConfig{
		Raft:          raft.Config{ID: cfg.ID, Peers: cfg.GroupPeers, ElectionTick: electionTick, HeartbeatTick: heartbeatTick},
		ListenAddress: cfg.ListenAddress,
		Peers:         cfg.TransportPeers,
		Persister:     raft.NewPersister(db),
		Apply:         r.apply,
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	r.host = host
	return r, nil
}

// apply decodes one committed range command and writes it into the durable engine. The
// entry's own Index is used as the command's MVCC WallTime: it is already a per-range
// strictly increasing sequence number by construction (Raft log indices never repeat or
// go backwards), so it is a correct, free version counter -- no separate clock is needed
// until cross-range transactions (Phase 8) require one comparable across ranges.
//
// Non-range-namespaced entries are ignored, not errored: Raft accepts arbitrary
// application entries, and a range host owns only its own namespaced commands (see
// rangeCommandPrefix in multiraft.go), leaving room for other command families sharing
// the same Raft group in a later phase.
func (r *DurableRange) apply(entry raft.Entry) error {
	command, ok, err := decodeRangeCommand(entry.Data)
	if err != nil || !ok {
		return err
	}
	ts := storage.HLC{WallTime: int64(entry.Index)}
	switch command.Type {
	case commandPut:
		return r.db.Put(command.Key, ts, command.Value)
	case commandDelete:
		return r.db.Delete(command.Key, ts)
	default:
		return errors.New("kv: unknown range command")
	}
}

// Tick drives this replica's election/heartbeat clock. The caller is responsible for
// calling it on a regular interval, matching internal/ann.DurableNode.Tick's contract.
func (r *DurableRange) Tick() error { return r.host.Tick() }

// Put proposes a key/value write. It only succeeds if this replica is currently the Raft
// leader, matching Host.Propose's contract -- see internal/ann.DurableNode.Insert's doc
// comment for the client-retry pattern this requires.
func (r *DurableRange) Put(key, value []byte) error {
	if err := validateRangeKey(key); err != nil {
		return err
	}
	data, err := marshalRangeCommand(rangeCommand{Type: commandPut, Key: key, Value: value})
	if err != nil {
		return err
	}
	return r.host.Propose(data)
}

// Delete proposes a key removal. See Put for the leader contract.
func (r *DurableRange) Delete(key []byte) error {
	if err := validateRangeKey(key); err != nil {
		return err
	}
	data, err := marshalRangeCommand(rangeCommand{Type: commandDelete, Key: key})
	if err != nil {
		return err
	}
	return r.host.Propose(data)
}

// Get reads the newest value this replica has itself applied for key. This is a
// bounded-staleness read, not a linearizable one: a replica can only ever return data it
// has actually applied, so it never returns a phantom or future value, but a replica that
// has fallen behind can return a value older than one a client already observed
// acknowledged elsewhere. That distinction matters for this project's own claims
// discipline (docs/correctness.md) -- it is exactly right for DurableNode.Search's
// reasoning (ANN search is approximate and bounded-staleness by design, so any replica's
// applied graph is a valid answer), but it is NOT sufficient on its own to justify calling
// the KV plane linearizable. A caller needing that guarantee wants ConsistentGet instead.
func (r *DurableRange) Get(key []byte) ([]byte, error) {
	return r.db.Get(key, storage.HLC{WallTime: math.MaxInt64})
}

// ConsistentGet first confirms this replica still has a Raft quorum, then reads key. A
// local leader role alone is insufficient: a former leader isolated by a partition can
// retain that role after a newer majority has committed writes elsewhere. Host.ReadIndex
// supplies the quorum proof without a clock assumption; it times out rather than serving
// a potentially stale answer when the leader cannot reach a majority.
func (r *DurableRange) ConsistentGet(key []byte) ([]byte, error) {
	if _, err := r.host.ReadIndex(readIndexTimeout); err != nil {
		return nil, err
	}
	return r.Get(key)
}

// validateRangeKey rejects an empty key or one that collides with Persister's reserved
// namespace in the same underlying engine.
func validateRangeKey(key []byte) error {
	if len(key) == 0 {
		return errors.New("kv: empty key")
	}
	if bytes.HasPrefix(key, reservedKeyPrefix) {
		return errors.New("kv: key collides with the reserved \"raft/\" namespace")
	}
	return nil
}

// Status reports this replica's own Raft role and term.
func (r *DurableRange) Status() (raft.Role, uint64) { return r.host.Status() }

// Addr returns this replica's bound transport address.
func (r *DurableRange) Addr() string { return r.host.Addr() }

// Close stops the transport and closes the storage engine. It does not delete StorageDir:
// reopening NewDurableRange against the same directory is the crash-restart-and-recover
// scenario this type exists to support.
func (r *DurableRange) Close() error {
	if err := r.host.Close(); err != nil {
		return err
	}
	return r.db.Close()
}
