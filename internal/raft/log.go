package raft

import "errors"

// raftLog holds the entries a node knows about. entries[0] is a placeholder (the entry
// just before the log starts, or the snapshot point), so real entries start at index 1.
type raftLog struct {
	entries            []Entry
	committed, applied uint64 // highest index known committed / handed to the state machine
	snapshot           Snapshot
}

func newLog() *raftLog { return &raftLog{entries: []Entry{{}}} }

func (l *raftLog) lastIndex() uint64 { return l.entries[len(l.entries)-1].Index }

// term returns the term of the entry at index i, and false if we don't have it.
//
// Entries are always contiguous (append never leaves gaps), so the entry for index i sits
// at position i - entries[0].Index. That makes this a direct lookup instead of a scan,
// which matters because it runs on every append response, including heartbeats.
func (l *raftLog) term(i uint64) (uint64, bool) {
	if i == l.snapshot.Index {
		return l.snapshot.Term, true
	}
	base := l.entries[0].Index
	if i < base {
		return 0, false
	}
	idx := i - base
	if idx >= uint64(len(l.entries)) {
		return 0, false
	}
	entry := l.entries[idx]
	if entry.Index != i {
		return 0, false
	}
	return entry.Term, true
}

// append adds entries to the log. If an incoming entry conflicts with one we already
// have (same index, different term), our entry and everything after it is dropped first.
func (l *raftLog) append(es []Entry) error {
	for _, e := range es {
		if e.Index == 0 {
			return errors.New("raft: zero index entry")
		}
		if e.Index <= l.lastIndex() {
			if old, ok := l.term(e.Index); ok && old == e.Term {
				continue // already have this exact entry
			}
			keep := 0
			for keep < len(l.entries) && l.entries[keep].Index < e.Index {
				keep++
			}
			l.entries = l.entries[:keep]
		}
		if e.Index != l.lastIndex()+1 {
			return errors.New("raft: non-contiguous append")
		}
		e.Data = append([]byte(nil), e.Data...)
		l.entries = append(l.entries, e)
	}
	return nil
}

// entriesFrom returns a copy of every entry from index i onward.
func (l *raftLog) entriesFrom(i uint64) []Entry {
	if i > l.lastIndex() {
		return nil
	}
	base := l.entries[0].Index
	start := 0
	if i > base {
		start = int(i - base)
	}
	out := make([]Entry, len(l.entries[start:]))
	copy(out, l.entries[start:])
	return out
}
