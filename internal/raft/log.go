package raft

import "errors"

type raftLog struct {
	entries            []Entry
	committed, applied uint64
	snapshot           Snapshot
}

func newLog() *raftLog               { return &raftLog{entries: []Entry{{}}} }
func (l *raftLog) lastIndex() uint64 { return l.entries[len(l.entries)-1].Index }
func (l *raftLog) term(i uint64) (uint64, bool) {
	if i == l.snapshot.Index {
		return l.snapshot.Term, true
	}
	for _, entry := range l.entries {
		if entry.Index == i {
			return entry.Term, true
		}
	}
	return 0, false
}
func (l *raftLog) append(es []Entry) error {
	for _, e := range es {
		if e.Index == 0 {
			return errors.New("raft: zero index entry")
		}
		if e.Index <= l.lastIndex() {
			if old, ok := l.term(e.Index); ok && old == e.Term {
				continue
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
func (l *raftLog) entriesFrom(i uint64) []Entry {
	if i > l.lastIndex() {
		return nil
	}
	start := 0
	for start < len(l.entries) && l.entries[start].Index < i {
		start++
	}
	out := make([]Entry, len(l.entries[start:]))
	copy(out, l.entries[start:])
	return out
}
