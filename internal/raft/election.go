package raft

func (n *node) startPreVote() {
	n.votes = map[NodeID]bool{n.id: true}
	n.preVote = true
	last, term := n.log.lastIndex(), uint64(0)
	term, _ = n.log.term(last)
	// Only voters are asked -- a learner's response could never legitimately count
	// (quorum() doesn't include it), and messaging it would just be a wasted round trip.
	for _, p := range n.voters() {
		if p != n.id {
			n.send(Message{Type: MsgPreVote, To: p, Term: n.term + 1, Index: last, LogTerm: term})
		}
	}
}
func (n *node) handlePreVote(m Message) {
	// A learner never grants a vote, even a pre-vote, even if a leader ever mistakenly
	// messaged one directly: it isn't part of this group's voting quorum, so any
	// election it might unwittingly help along cannot be safety-checked against
	// quorum() the way a real voter's grant can. Defense in depth -- startPreVote/
	// startElection never message a learner in the first place -- not a load-bearing
	// path in the current code, but a learner voting would silently break the
	// "quorum() counts only voters" invariant everywhere else in this file relies on.
	if n.isLearner {
		n.send(Message{Type: MsgPreVoteResp, To: m.From, Term: n.term, Reject: true})
		return
	}
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
	for _, p := range n.voters() {
		if p != n.id {
			n.send(Message{Type: MsgVote, To: p, Term: n.term, Index: last, LogTerm: term})
		}
	}
}
func (n *node) handleVote(m Message) {
	// See handlePreVote's comment: a learner never grants a real vote either, for the
	// identical reason.
	if n.isLearner {
		n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Reject: true})
		return
	}
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
