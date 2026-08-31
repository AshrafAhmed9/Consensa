package raft

func (n *node) startPreVote() {
	n.votes = map[NodeID]bool{n.id: true}
	n.preVote = true
	last, term := n.log.lastIndex(), uint64(0)
	term, _ = n.log.term(last)
	for _, p := range n.peers {
		if p != n.id {
			n.send(Message{Type: MsgPreVote, To: p, Term: n.term + 1, Index: last, LogTerm: term})
		}
	}
}
func (n *node) handlePreVote(m Message) {
	last, term := n.log.lastIndex(), uint64(0)
	term, _ = n.log.term(last)
	ok := m.Term >= n.term+1 && (m.LogTerm > term || m.LogTerm == term && m.Index >= last)
	n.send(Message{Type: MsgPreVoteResp, To: m.From, Term: n.term, Reject: !ok})
}
func (n *node) handlePreVoteResp(m Message) {
	if m.Reject || m.Term > n.term {
		return
	}
	if !n.preVote || n.votes == nil {
		return
	}
	n.votes[m.From] = true
	if len(n.votes) >= n.quorum() {
		n.startElection()
	}
}
func (n *node) startElection() {
	n.preVote = false
	n.role = Candidate
	n.term++
	n.vote = uint64(n.id)
	n.votes = map[NodeID]bool{n.id: true}
	last, term := n.log.lastIndex(), uint64(0)
	term, _ = n.log.term(last)
	for _, p := range n.peers {
		if p != n.id {
			n.send(Message{Type: MsgVote, To: p, Term: n.term, Index: last, LogTerm: term})
		}
	}
}
func (n *node) handleVote(m Message) {
	last, term := n.log.lastIndex(), uint64(0)
	term, _ = n.log.term(last)
	ok := (n.vote == 0 || n.vote == uint64(m.From)) && (m.LogTerm > term || m.LogTerm == term && m.Index >= last)
	if ok {
		n.vote = uint64(m.From)
		n.electionElapsed = 0
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Reject: !ok})
}
func (n *node) handleVoteResp(m Message) {
	if n.role != Candidate || m.Term != n.term {
		return
	}
	if !m.Reject {
		n.votes[m.From] = true
		if len(n.votes) >= n.quorum() {
			n.becomeLeader()
		}
	}
}
