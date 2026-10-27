package raft

// handleSnapshot installs a snapshot sent by the leader. The snapshot replaces the whole
// log up to its index, so the log restarts from that point.
func (n *node) handleSnapshot(m Message) {
	s := m.Snapshot
	if s.Index <= n.log.committed {
		return // we already have everything the snapshot covers
	}
	n.becomeFollower(m.Term, m.From)
	n.log.snapshot = s
	n.log.entries = []Entry{{Index: s.Index, Term: s.Term}}
	n.log.committed = s.Index
	n.log.applied = s.Index
	n.restoreConfState(s.ConfState, s.Index)
}
