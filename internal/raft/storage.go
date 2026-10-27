package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"sort"

	"github.com/ashraf/consensa/internal/storage"
)

var hardStateKey = []byte("raft/hard-state")

// Persister saves a node's durable state (term, vote and log) to the storage
// engine. It must run before the node's messages are sent.
type Persister struct{ db *storage.DB }

// NewPersister saves Raft state into db.
func NewPersister(db *storage.DB) *Persister { return &Persister{db: db} }

// Persist saves the hard state and every new log entry, then syncs the
// write-ahead log once so all of it is on disk before the caller sends any message.
func (p *Persister) Persist(ready Ready) error {
	if data, err := json.Marshal(ready.HardState); err != nil {
		return err
	} else if err := p.db.Put(hardStateKey, data); err != nil {
		return err
	}
	for _, entry := range ready.Entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if err := p.db.Put(entryKey(entry.Index), data); err != nil {
			return err
		}
	}
	return p.db.Sync()
}

// LoadHardState reads back the saved term, vote and commit index.
func (p *Persister) LoadHardState() (HardState, error) {
	data, err := p.db.Get(hardStateKey)
	if err != nil {
		return HardState{}, err
	}
	var state HardState
	return state, json.Unmarshal(data, &state)
}

// LoadEntries reads back every saved log entry, in index order.
func (p *Persister) LoadEntries() ([]Entry, error) {
	var entries []Entry
	for _, kv := range p.db.Scan([]byte("raft/log/"), []byte("raft/log0")) {
		var entry Entry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Index < entries[j].Index })
	return entries, nil
}

// RecoverNode builds a node from whatever the Persister saved (or a fresh node if nothing
// was saved). Entries that were committed but may not have been applied before the
// crash are handed out again in Ready, so the state machine catches up.
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
	entries, err := persister.LoadEntries()
	if err != nil {
		return nil, err
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
	// The log may hold a newer config entry than the snapshot does.
	recovered.recomputeMembership()
	return recovered, nil
}

// entryKey is the storage key for a log entry. The index is written as 8 big-endian
// bytes so keys sort in index order.
func entryKey(index uint64) []byte {
	key := []byte("raft/log/")
	return binary.BigEndian.AppendUint64(key, index)
}
