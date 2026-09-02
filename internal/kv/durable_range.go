package kv

import (
	"bytes"
	"errors"
	"math"
	"sync"
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
	id   raft.NodeID
	host *raft.Host
	db   *storage.DB

	// leaseMu guards currentLease: apply (Host's own goroutine) writes it as lease-grant
	// commands commit, while CurrentLease is read from whatever goroutine a caller uses
	// to decide if a follower read is safe -- the same producer/single-or-many-consumer
	// shape as Go's sync.RWMutex is built for.
	leaseMu      sync.RWMutex
	currentLease Lease

	// closedMu guards closedTimestamp and appliedIndex the same way leaseMu guards
	// currentLease, and for the same reason: apply's writer goroutine versus an arbitrary
	// reader goroutine deciding whether a follower read is currently safe.
	closedMu         sync.RWMutex
	closedTimestamp  ClosedTimestamp
	lastAppliedIndex uint64
}

// DurableRangeConfig names one replica's identity, group membership, and durable storage.
type DurableRangeConfig struct {
	ID             raft.NodeID
	GroupPeers     []raft.NodeID
	Learners       []raft.NodeID
	ListenAddress  string
	TransportPeers map[raft.NodeID]string
	// Transport optionally attaches this range to a logical view of a shared listener.
	// When nil, NewDurableRange creates its historical dedicated TCP listener.
	Transport     raft.Transport
	StorageDir    string
	ElectionTick  int
	HeartbeatTick int
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
	r := &DurableRange{id: cfg.ID, db: db}

	electionTick, heartbeatTick := cfg.ElectionTick, cfg.HeartbeatTick
	if electionTick == 0 {
		electionTick = 10
	}
	if heartbeatTick == 0 {
		heartbeatTick = 2
	}

	host, err := raft.NewHost(raft.HostConfig{
		Raft:          raft.Config{ID: cfg.ID, Peers: cfg.GroupPeers, Learners: cfg.Learners, ElectionTick: electionTick, HeartbeatTick: heartbeatTick},
		ListenAddress: cfg.ListenAddress,
		Peers:         cfg.TransportPeers,
		Transport:     cfg.Transport,
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
	// Tracked unconditionally, before the decode/namespace check below: "how far has
	// this replica applied" must reflect every committed entry it has processed, not
	// only ones this range recognized -- a foreign command family sharing this Raft
	// group (see rangeCommandPrefix's own doc comment) still advances real progress.
	r.closedMu.Lock()
	if entry.Index > r.lastAppliedIndex {
		r.lastAppliedIndex = entry.Index
	}
	r.closedMu.Unlock()

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
	case commandLease:
		r.leaseMu.Lock()
		r.currentLease = Lease{Holder: command.LeaseHolder, Start: command.LeaseStart, Expiration: command.LeaseExpiration}
		r.leaseMu.Unlock()
		return nil
	case commandClosedTimestamp:
		r.closedMu.Lock()
		r.closedTimestamp = ClosedTimestamp{Timestamp: command.ClosedTimestamp, AppliedIndex: entry.Index}
		r.closedMu.Unlock()
		return nil
	default:
		return errors.New("kv: unknown range command")
	}
}

// Tick drives this replica's election/heartbeat clock. The caller is responsible for
// calling it on a regular interval, matching internal/ann.DurableNode.Tick's contract.
func (r *DurableRange) Tick() error { return r.host.Tick() }

// ProposeConfChange changes voters among the transport-known peer universe. It is an
// operator primitive: callers must first start and catch up a learner, then propose its
// promotion through the current leader.
//
// "Transport-known" is the operative phrase: a genuinely new process this deployment has
// never addressed before must have its address registered on every existing replica via
// AddPeerAddress before (or alongside) this call adds its ID to the group -- see
// AddPeerAddress's own doc comment for why these are two separate steps.
func (r *DurableRange) ProposeConfChange(voters, learners []raft.NodeID) error {
	return r.host.ProposeConfChange(voters, learners)
}

// AddKnownPeer extends this replica's local Raft peer universe to include id, the
// companion step to AddPeerAddress below: ProposeConfChange rejects any ID not already
// known (raft.Node.AddKnownPeer's own doc comment), so provisioning a genuinely new
// process needs both this and AddPeerAddress called on every existing replica first.
func (r *DurableRange) AddKnownPeer(id raft.NodeID) {
	r.host.AddKnownPeer(id)
}

// AddPeerAddress registers a new replica's transport address on this replica, so that
// once ProposeConfChange (above) adds that replica's ID to the group, real messages can
// actually reach it. This is the piece docs/notes/11-joint-consensus.md named as
// missing: "no workflow yet for provisioning a brand-new process and publishing its
// address." It must be called on every existing replica -- not just the leader --
// because every replica that ends up needing to send the new one a message (a heartbeat,
// a snapshot, a vote request) independently needs its own transport to know the address;
// there is no cluster-wide broadcast of this call.
func (r *DurableRange) AddPeerAddress(id raft.NodeID, address string) error {
	return r.host.AddPeer(id, address)
}

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

// AllKeys returns every application key/value this replica has currently applied,
// excluding Persister's reserved "raft/" namespace -- a full scan of this replica's
// local engine, not a Raft operation. It exists for split-key selection and the live
// data migration a range split performs (see split.go and durable_split_test.go): both
// need to see the range's actual current content, not just individual keys.
func (r *DurableRange) AllKeys() (map[string][]byte, error) {
	it := r.db.Scan(nil, nil, storage.HLC{WallTime: math.MaxInt64})
	defer func() { _ = it.Close() }()
	out := map[string][]byte{}
	for it.Next() {
		key := it.Key()
		if bytes.HasPrefix(key, reservedKeyPrefix) {
			continue
		}
		out[string(key)] = append([]byte(nil), it.Value()...)
	}
	return out, it.Err()
}

// MaybeSplitKey applies ShouldSplit (split.go) against this replica's own currently
// applied data, so a caller deciding whether to split doesn't have to call AllKeys and
// ShouldSplit separately. Like AllKeys, this is a local read of whatever this replica has
// applied, not a Raft operation, and it only answers the decision question -- see
// ShouldSplit's own doc comment for what still has to happen after this returns true.
func (r *DurableRange) MaybeSplitKey(threshold int) ([]byte, bool, error) {
	keys, err := r.AllKeys()
	if err != nil {
		return nil, false, err
	}
	splitKey, ok := ShouldSplit(threshold, keys)
	return splitKey, ok, nil
}

// GrantLease proposes a lease authorizing holder to serve follower reads over
// [now, now+duration) once the proposal commits and every replica applies it. Like Put,
// this only succeeds if this replica is currently the Raft leader -- a lease is only
// meaningful if the whole group agrees who granted it, and only the leader's proposals
// can commit. The timestamps are fixed once here by the proposer, not recomputed per
// replica on apply, so every replica ends up with the identical interval regardless of
// how staggered their individual apply calls are.
//
// See AdvanceClosedTimestamp and FollowerRead below for what a granted lease actually
// authorizes; GrantLease itself only closes lease replication.
func (r *DurableRange) GrantLease(holder raft.NodeID, duration time.Duration) error {
	if duration <= 0 {
		return errors.New("kv: lease duration must be positive")
	}
	now := time.Now()
	data, err := marshalRangeCommand(rangeCommand{
		Type: commandLease, LeaseHolder: holder, LeaseStart: now, LeaseExpiration: now.Add(duration),
	})
	if err != nil {
		return err
	}
	return r.host.Propose(data)
}

// CurrentLease returns the most recent lease this replica has applied. The zero Lease
// (no holder, zero times) means no lease has ever been granted and applied here yet --
// ValidAt/ValidWithOffset both correctly reject it, since a zero Expiration is never
// after any real now.
func (r *DurableRange) CurrentLease() Lease {
	r.leaseMu.RLock()
	defer r.leaseMu.RUnlock()
	return r.currentLease
}

// AdvanceClosedTimestamp proposes a promise that no future write at or below ts will land
// below this proposal's own eventual commit index -- the piece GrantLease's own doc
// comment named as still missing. Like Put and GrantLease, this only succeeds from the
// current leader: only the leader knows it has stopped (and will continue to stop)
// accepting writes at or below ts before proposing this. Callers are expected to call
// this periodically (mirroring a real production closed-timestamp tracker) with ts set
// slightly behind "now" -- how far behind is a latency/staleness tradeoff this type
// deliberately leaves to the caller rather than picking one policy here.
func (r *DurableRange) AdvanceClosedTimestamp(ts time.Time) error {
	data, err := marshalRangeCommand(rangeCommand{Type: commandClosedTimestamp, ClosedTimestamp: ts})
	if err != nil {
		return err
	}
	return r.host.Propose(data)
}

// CurrentClosedTimestamp returns the most recent closed-timestamp promise this replica has
// itself applied, paired with the local applied index at which it applied it (see apply's
// own doc comment for why AppliedIndex is per-replica even though Timestamp is not). The
// zero value (AppliedIndex 0) means nothing has been advanced and applied here yet, which
// FollowerReadAllowed/FollowerReadAllowedWithOffset both correctly treat as "not caught up
// to any promise."
func (r *DurableRange) CurrentClosedTimestamp() ClosedTimestamp {
	r.closedMu.RLock()
	defer r.closedMu.RUnlock()
	return r.closedTimestamp
}

// AppliedIndex returns this replica's own most recently applied Raft log index -- the
// piece docs/notes/09-leases.md named as missing before closed-timestamp advancement could
// be checked at all ("Host.AppliedIndex() does not exist yet"). Tracked directly by this
// type rather than added to raft.Host: DurableRange already observes every entry through
// its own Apply callback, so no change to Host's own surface was needed to get it.
func (r *DurableRange) AppliedIndex() uint64 {
	r.closedMu.RLock()
	defer r.closedMu.RUnlock()
	return r.lastAppliedIndex
}

// FollowerRead serves key from this replica's own local storage if -- and only if --
// FollowerReadAllowedWithOffset (lease.go) says doing so is safe: this replica must
// currently hold a lease valid at now (within maxOffset's conservative clock-skew bound),
// must have applied at least as far as the closed timestamp's promised index, and readAt
// must not exceed the closed timestamp's promise. This is what actually closes the read
// path ladder's third rung (docs/notes/09-leases.md): ReadIndex proves quorum with no
// clock assumption on the leader; a lease-and-closed-timestamp read like this one instead
// lets ANY replica holding the lease -- follower included -- answer without contacting
// anyone, at the cost of the bounded clock-offset assumption ADR-009 states explicitly.
func (r *DurableRange) FollowerRead(key []byte, readAt time.Time, maxOffset time.Duration) ([]byte, error) {
	lease := r.CurrentLease()
	closed := r.CurrentClosedTimestamp()
	applied := r.AppliedIndex()
	if err := FollowerReadAllowedWithOffset(lease, r.id, closed, readAt, time.Now(), applied, maxOffset); err != nil {
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
