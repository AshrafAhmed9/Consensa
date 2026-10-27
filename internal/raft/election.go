package raft

// lastLog returns the index and term of this node's newest log entry. Votes compare these
// so a candidate with an older log can never win.
func (n *node) lastLog() (index, term uint64) {
	index = n.log.lastIndex()
	term, _ = n.log.term(index)
	return index, term
}

// logUpToDate reports whether a candidate's log (lastIndex, lastTerm) is at least as new
// as ours. A higher last term wins; with equal terms the longer log wins.
func (n *node) logUpToDate(lastIndex, lastTerm uint64) bool {
	index, term := n.lastLog()
	return lastTerm > term || lastTerm == term && lastIndex >= index
}

// startPreVote asks the voters whether an election would succeed, without changing our
// term. This stops a node that was cut off from the group from forcing a new election
// (and bumping everyone's term) when it comes back.
func (n *node) startPreVote() {
	n.votes = map[NodeID]bool{n.id: true}
	n.preVote = true
	last, term := n.lastLog()
	for _, p := range n.votingPeers() {
		n.send(Message{Type: MsgPreVote, To: p, Term: n.term + 1, Index: last, LogTerm: term})
	}
}

// handlePreVote answers a pre-vote: yes if the candidate's log is up to date. It never
// changes our own term or vote.
func (n *node) handlePreVote(m Message) {
	if !n.isVoter(n.id) {
		n.send(Message{Type: MsgPreVoteResp, To: m.From, Term: n.term, Reject: true})
		return
	}
	ok := m.Term >= n.term+1 && n.logUpToDate(m.Index, m.LogTerm)
	n.send(Message{Type: MsgPreVoteResp, To: m.From, Term: n.term, Reject: !ok})
}

// handlePreVoteResp counts pre-vote answers and starts the real election once we have a quorum.
func (n *node) handlePreVoteResp(m Message) {
	if m.Reject || m.Term > n.term {
		return
	}
	if !n.preVote || n.votes == nil {
		return
	}
	n.votes[m.From] = true
	if n.membership.HasQuorum(n.votes) {
		n.startElection()
	}
}

// startElection moves to a new term, votes for ourselves, and asks the voters for their votes.
func (n *node) startElection() {
	n.preVote = false
	n.role = Candidate
	n.term++
	n.vote = uint64(n.id)
	n.votes = map[NodeID]bool{n.id: true}
	last, term := n.lastLog()
	for _, p := range n.votingPeers() {
		n.send(Message{Type: MsgVote, To: p, Term: n.term, Index: last, LogTerm: term})
	}
}

// handleVote grants our vote if we haven't voted for someone else this term and the
// candidate's log is up to date. Non-voters never vote.
func (n *node) handleVote(m Message) {
	if !n.isVoter(n.id) {
		n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Reject: true})
		return
	}
	ok := (n.vote == 0 || n.vote == uint64(m.From)) && n.logUpToDate(m.Index, m.LogTerm)
	if ok {
		n.vote = uint64(m.From)
		n.electionElapsed = 0
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Reject: !ok})
}

// handleVoteResp counts votes and becomes leader once we have a quorum.
func (n *node) handleVoteResp(m Message) {
	if n.role != Candidate || m.Term != n.term {
		return
	}
	if !m.Reject {
		n.votes[m.From] = true
		if n.membership.HasQuorum(n.votes) {
			n.becomeLeader()
		}
	}
}
