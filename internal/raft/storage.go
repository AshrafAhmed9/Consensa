package raft

import (
	"encoding/json"
	"errors"
	"math"
	"sort"

	"github.com/ashraf/consensa/internal/storage"
)

var hardStateKey = []byte("raft/hard-state")
var snapshotKey = []byte("raft/snapshot")

// Persister writes the durable portion of Ready to the LSM before its Messages may be sent.
// The caller owns the ordering: Persist(ready) -> transport send -> state-machine apply -> Advance.
type Persister struct{ engine storage.Engine }

// NewPersister binds a Raft group to the durable Phase 1 storage interface.
func NewPersister(engine storage.Engine) *Persister { return &Persister{engine: engine} }

// Persist stores HardState and every unstable entry. The max timestamp makes these metadata
// records independent of client MVCC timestamps while preserving ordinary Engine semantics.
func (p *Persister) Persist(ready Ready) error {
	if data, err := json.Marshal(ready.HardState); err != nil {
		return err
	} else if err := p.engine.Put(hardStateKey, storage.HLC{WallTime: math.MaxInt64}, data); err != nil {
		return err
	}
	if ready.Snapshot.Index != 0 {
		data, err := json.Marshal(ready.Snapshot)
		if err != nil {
			return err
		}
		if err := p.engine.Put(snapshotKey, storage.HLC{WallTime: math.MaxInt64}, data); err != nil {
			return err
		}
	}
	for _, entry := range ready.Entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if err := p.engine.Put(entryKey(entry.Index), storage.HLC{WallTime: math.MaxInt64}, data); err != nil {
			return err
		}
	}
	return p.engine.Flush()
}

// LoadHardState restores the last durable term, vote, and commit marker.
func (p *Persister) LoadHardState() (HardState, error) {
	data, err := p.engine.Get(hardStateKey, storage.HLC{WallTime: math.MaxInt64})
	if err != nil {
		return HardState{}, err
	}
	var state HardState
	return state, json.Unmarshal(data, &state)
}

// LoadSnapshot restores the last installed snapshot, if one has been persisted.
func (p *Persister) LoadSnapshot() (Snapshot, error) {
	data, err := p.engine.Get(snapshotKey, storage.HLC{WallTime: math.MaxInt64})
	if err != nil {
		return Snapshot{}, err
	}
	var snapshot Snapshot
	return snapshot, json.Unmarshal(data, &snapshot)
}

// LoadEntries restores the durable log suffix in index order. Snapshot restoration omits
// entries at or before its index, so callers can rebuild the same logical log without
// replaying compacted history.
func (p *Persister) LoadEntries() ([]Entry, error) {
	it := p.engine.Scan([]byte("raft/log/"), []byte("raft/log0"), storage.HLC{WallTime: math.MaxInt64})
	defer func() { _ = it.Close() }()
	var entries []Entry
	for it.Next() {
		var entry Entry
		if err := json.Unmarshal(it.Value(), &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Index < entries[j].Index })
	return entries, nil
}

// RecoverNode reconstructs a pure Raft node from state previously persisted by Persister.
// Recovery deliberately re-exposes committed-but-unapplied entries in Ready: the caller's
// state machine may have crashed after the log commit became durable but before Apply.
func RecoverNode(config Config, persister *Persister) (Node, error) {
	if persister == nil {
		return nil, errors.New("raft: nil persister")
	}
	created, err := NewNode(config)
	if err != nil {
		return nil, err
	}
	recovered := created.(*node)
	hardState, err := persister.LoadHardState()
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}
	snapshot, err := persister.LoadSnapshot()
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}
	entries, err := persister.LoadEntries()
	if err != nil {
		return nil, err
	}
	if snapshot.Index > 0 {
		recovered.log.snapshot = snapshot
		recovered.log.entries = []Entry{{Index: snapshot.Index, Term: snapshot.Term}}
		recovered.log.applied = snapshot.Index
		recovered.restoreConfState(snapshot.ConfState, snapshot.Index)
	}
	for _, entry := range entries {
		if entry.Index <= recovered.log.lastIndex() {
			continue
		}
		if err := recovered.log.append([]Entry{entry}); err != nil {
			return nil, err
		}
	}
	if hardState.Commit > recovered.log.lastIndex() {
		return nil, errors.New("raft: committed index exceeds persisted log")
	}
	recovered.term = hardState.Term
	recovered.vote = hardState.Vote
	recovered.log.committed = hardState.Commit
	recovered.lastHard = hardState
	// Any surviving suffix may contain a later configuration entry than the snapshot.
	recovered.recomputeMembership()
	return recovered, nil
}

func entryKey(index uint64) []byte { return []byte("raft/log/" + formatIndex(index)) }
func formatIndex(index uint64) string {
	return string([]byte{byte(index >> 56), byte(index >> 48), byte(index >> 40), byte(index >> 32), byte(index >> 24), byte(index >> 16), byte(index >> 8), byte(index)})
}
