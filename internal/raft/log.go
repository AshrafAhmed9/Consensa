package raft

import "errors"

type raftLog struct {
	entries            []Entry
	committed, applied uint64
	snapshot           Snapshot
}

func newLog() *raftLog               { return &raftLog{entries: []Entry{{}}} }
func (l *raftLog) lastIndex() uint64 { return l.entries[len(l.entries)-1].Index }
// term looks up an entry's term by direct index arithmetic rather than a linear scan:
// append enforces that every new entry's Index is exactly lastIndex()+1 (strictly
// contiguous, never a gap), and truncation only ever removes a contiguous suffix, so
// entries[i].Index == entries[0].Index+i holds as an invariant for the whole slice at
// every point in this type's lifetime. That makes this O(1) instead of O(len(entries)),
// which matters because it is called from advanceCommit -- itself invoked on every
// single AppendEntries response, including plain heartbeats -- so its own cost multiplies
// directly into the hot replication path. Found as a real, reproducible CI/local
// regression, not by inspection: joint consensus's advanceCommit (node.go) checks
// multiple candidate commit indices per call instead of the prior single-quorum
// implementation's one, and under a continuously growing log (e.g. periodic
// AdvanceClosedTimestamp proposals) the multiplied linear-scan cost was enough to
// destabilize leadership in cmd/consensa's own three-process end-to-end test.
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
	base := l.entries[0].Index
	start := 0
	if i > base {
		start = int(i - base)
	}
	out := make([]Entry, len(l.entries[start:]))
	copy(out, l.entries[start:])
	return out
}
