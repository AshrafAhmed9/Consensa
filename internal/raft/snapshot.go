package raft

func (n *node) handleSnapshot(m Message) {
	s := m.Snapshot
	if s.Index <= n.log.committed {
		return
	}
	n.becomeFollower(m.Term, m.From)
	n.log.snapshot = s
	n.log.entries = []Entry{{Index: s.Index, Term: s.Term}}
	n.log.committed = s.Index
	n.log.applied = s.Index
}
