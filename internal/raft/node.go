package raft

import (
	"errors"
	"sort"
)

// Ready describes work outside the pure state machine. Persist HardState and Entries
// before sending Messages: this ordering is Raft's durable-vote and durable-log safety boundary.
type Ready struct {
	HardState        HardState
	Entries          []Entry
	Snapshot         Snapshot
	CommittedEntries []Entry
	Messages         []Message
}

// Node is a deterministic Raft replica. Time advances only through Tick and no method
// blocks, which lets the simulator enumerate delivery schedules exactly.
type Node interface {
	Step(Message) error
	Tick()
	Propose([]byte) error
	Ready() Ready
	Advance()
	// Status reports this replica's own view of its role and term, for administrative
	// and diagnostic surfaces. It is read-only and never changes protocol behavior.
	Status() (Role, uint64)
}
type node struct {
	id                                                             NodeID
	peers                                                          []NodeID
	learners                                                       map[NodeID]bool
	isLearner                                                      bool
	role                                                           Role
	term, vote, leader                                             uint64
	log                                                            *raftLog
	electionElapsed, heartbeatElapsed, electionTick, heartbeatTick int
	votes                                                          map[NodeID]bool
	preVote                                                        bool
	next, match                                                    map[NodeID]uint64
	msgs                                                           []Message
	unstable                                                       []Entry
	lastHard                                                       HardState
}

// NewNode creates a follower with a sentinel log entry.
func NewNode(c Config) (Node, error) {
	if c.ID == 0 || len(c.Peers) == 0 {
		return nil, errors.New("raft: ID and peers required")
	}
	if c.ElectionTick <= c.HeartbeatTick || c.HeartbeatTick <= 0 {
		return nil, errors.New("raft: election tick must exceed heartbeat tick")
	}
	found := false
	for _, p := range c.Peers {
		found = found || p == c.ID
	}
	if !found {
		return nil, errors.New("raft: local ID absent from peers")
	}
	learners := map[NodeID]bool{}
	for _, l := range c.Learners {
		isPeer := false
		for _, p := range c.Peers {
			isPeer = isPeer || p == l
		}
		if !isPeer {
			return nil, errors.New("raft: learner must also be a peer")
		}
		learners[l] = true
	}
	if len(learners) >= len(c.Peers) {
		return nil, errors.New("raft: at least one peer must be a voter")
	}
	return &node{
		id: c.ID, peers: append([]NodeID(nil), c.Peers...), learners: learners, isLearner: learners[c.ID],
		log: newLog(), electionTick: c.ElectionTick, heartbeatTick: c.HeartbeatTick,
		next: map[NodeID]uint64{}, match: map[NodeID]uint64{},
	}, nil
}

// voters returns peers excluding learners -- the set every quorum computation (elections,
// commit advancement) counts over. Learners still appear in n.peers so broadcastAppend
// still replicates the log to them; they are excluded here, not there.
func (n *node) voters() []NodeID {
	out := make([]NodeID, 0, len(n.peers))
	for _, p := range n.peers {
		if !n.learners[p] {
			out = append(out, p)
		}
	}
	return out
}

func (n *node) quorum() int { return len(n.voters())/2 + 1 }
func (n *node) Tick() {
	n.electionElapsed++
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTick {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
	}
	// A learner never starts an election, no matter how long it goes without hearing
	// from a leader: it isn't a voter, so it could never actually win one (no peer would
	// count its vote request toward quorum -- see startPreVote/startElection below,
	// which only message n.voters()) and letting it try would just be wasted messages on
	// every election timeout for as long as it stays a learner.
	if n.electionElapsed >= n.electionTick && n.role != Leader && !n.isLearner {
		n.electionElapsed = 0
		n.startPreVote()
	}
}
func (n *node) Propose(data []byte) error {
	if n.role != Leader {
		return errors.New("raft: proposal to non-leader")
	}
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Data: append([]byte(nil), data...)}
	if e := n.log.append([]Entry{e}); e != nil {
		return e
	}
	n.unstable = append(n.unstable, e)
	n.match[n.id] = n.log.lastIndex()
	n.broadcastAppend()
	return nil
}
func (n *node) Step(m Message) error {
	if m.To != 0 && m.To != n.id {
		return errors.New("raft: wrong destination")
	}
	// Pre-vote asks whether an election would be viable; adopting its prospective term
	// would defeat pre-vote by letting an isolated node disrupt a healthy leader.
	if m.Type != MsgPreVote && m.Type != MsgPreVoteResp && m.Term > n.term {
		n.becomeFollower(m.Term, 0)
	}
	switch m.Type {
	case MsgPreVote:
		n.handlePreVote(m)
	case MsgPreVoteResp:
		n.handlePreVoteResp(m)
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgAppend:
		n.handleAppend(m)
	case MsgAppendResp:
		n.handleAppendResp(m)
	case MsgSnapshot:
		n.handleSnapshot(m)
	default:
		return errors.New("raft: unknown message")
	}
	return nil
}
func (n *node) Ready() Ready {
	r := Ready{HardState: HardState{Term: n.term, Vote: n.vote, Commit: n.log.committed}, Entries: cloneEntries(n.unstable), Messages: append([]Message(nil), n.msgs...)}
	if n.log.applied < n.log.committed {
		r.CommittedEntries = n.log.entriesFrom(n.log.applied + 1)
		if len(r.CommittedEntries) > int(n.log.committed-n.log.applied) {
			r.CommittedEntries = r.CommittedEntries[:n.log.committed-n.log.applied]
		}
	}
	return r
}
func (n *node) Advance() {
	n.unstable = nil
	n.msgs = nil
	n.log.applied = n.log.committed
	n.lastHard = HardState{Term: n.term, Vote: n.vote, Commit: n.log.committed}
}

func (n *node) Status() (Role, uint64) { return n.role, n.term }
func (n *node) send(m Message)         { m.From = n.id; n.msgs = append(n.msgs, m) }
func (n *node) becomeFollower(term uint64, leader NodeID) {
	n.role = Follower
	n.term = term
	n.vote = 0
	n.leader = uint64(leader)
	n.votes = nil
	n.preVote = false
	n.electionElapsed = 0
}
func (n *node) becomeLeader() {
	n.role = Leader
	n.leader = uint64(n.id)
	n.next = map[NodeID]uint64{}
	n.match = map[NodeID]uint64{n.id: n.log.lastIndex()}
	n.preVote = false
	for _, p := range n.peers {
		n.next[p] = n.log.lastIndex() + 1
	}
	n.broadcastAppend()
}
func (n *node) broadcastAppend() {
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
}
func (n *node) sendAppend(to NodeID) {
	next := n.next[to]
	prev := next - 1
	pt, _ := n.log.term(prev)
	n.send(Message{Type: MsgAppend, To: to, Term: n.term, Index: prev, LogTerm: pt, Commit: n.log.committed, Entries: n.log.entriesFrom(next)})
}
func (n *node) handleAppend(m Message) {
	if m.Term < n.term {
		n.send(Message{Type: MsgAppendResp, To: m.From, Term: n.term, Reject: true, Index: n.log.lastIndex()})
		return
	}
	n.becomeFollower(m.Term, m.From)
	if term, ok := n.log.term(m.Index); !ok || term != m.LogTerm {
		n.send(Message{Type: MsgAppendResp, To: m.From, Term: n.term, Reject: true, Index: n.log.lastIndex()})
		return
	}
	if err := n.log.append(m.Entries); err != nil {
		return
	}
	if len(m.Entries) > 0 {
		n.unstable = append(n.unstable, cloneEntries(m.Entries)...)
	}
	if m.Commit > n.log.committed {
		n.log.committed = min(m.Commit, n.log.lastIndex())
	}
	n.send(Message{Type: MsgAppendResp, To: m.From, Term: n.term, Index: n.log.lastIndex()})
}
func (n *node) handleAppendResp(m Message) {
	if n.role != Leader || m.Term != n.term {
		return
	}
	if m.Reject {
		if n.next[m.From] > 1 {
			n.next[m.From]--
		}
		n.sendAppend(m.From)
		return
	}
	n.match[m.From] = m.Index
	n.next[m.From] = m.Index + 1
	n.advanceCommit()
	if n.next[m.From] <= n.log.lastIndex() {
		n.sendAppend(m.From)
	}
}
func (n *node) advanceCommit() {
	// Only voters count toward what's committed -- Raft §5.4.2's safety property is
	// "replicated on a majority of VOTERS," not "replicated on a majority of anything
	// receiving the log." A learner's match progress still gets tracked (so it can be
	// promoted once caught up) but must never let an entry be counted committed on the
	// strength of learners alone, which would let a value be visible before it's actually
	// safe against a leader election among the real voters.
	voters := n.voters()
	matches := make([]uint64, 0, len(voters))
	for _, p := range voters {
		matches = append(matches, n.match[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] < matches[j] })
	candidate := matches[len(matches)-n.quorum()]
	term, _ := n.log.term(candidate)
	if candidate > n.log.committed && term == n.term {
		n.log.committed = candidate
		n.broadcastAppend()
	}
}
func cloneEntries(in []Entry) []Entry {
	out := make([]Entry, len(in))
	for i, e := range in {
		out[i] = Entry{Index: e.Index, Term: e.Term, Data: append([]byte(nil), e.Data...)}
	}
	return out
}
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
