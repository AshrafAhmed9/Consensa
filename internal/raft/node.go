package raft

import (
	"errors"
	"sort"
)

// Ready is the work the caller must do after the node has changed state. Do it in this
// order: save HardState and Entries to disk, then send Messages, then apply
// CommittedEntries. Saving before sending is what keeps votes and log entries from
// being lost in a crash.
type Ready struct {
	HardState        HardState
	Entries          []Entry
	Snapshot         Snapshot
	CommittedEntries []Entry
	Messages         []Message
}

// Node is one Raft replica. It does no I/O and starts no goroutines: time moves only when
// Tick is called, and messages come in through Step and go out through Ready. That makes
// it easy to test.
type Node interface {
	// Step handles one message from another node.
	Step(Message) error
	// Tick advances the election and heartbeat clocks by one.
	Tick()
	// Propose appends data to the log. Only the leader accepts it.
	Propose([]byte) error
	// ProposeConfChange starts a change of voters/learners using joint consensus (see
	// confchange.go). Only the leader may call it, and only when no change is already
	// running. The leader finishes the change itself once the first step commits.
	ProposeConfChange(newVoters, newLearners []NodeID) error
	// AddKnownPeer tells this node that id exists, which ProposeConfChange requires
	// before id can be added as a voter or learner. It is local bookkeeping, not
	// replicated, so call it on every node. It does nothing if id is already known.
	AddKnownPeer(id NodeID)
	// Ready returns the pending work, and Advance marks it as done.
	Ready() Ready
	Advance()
	// Status returns this node's role and term.
	Status() (Role, uint64)
	// Leader returns who this node believes the leader is, or 0 if it doesn't know. This
	// is a local guess and can be out of date.
	Leader() NodeID
	// ConfState returns the voter configuration to store with a snapshot.
	ConfState() ConfState
}

type node struct {
	id    NodeID
	peers []NodeID

	// initialMembership is the configuration the node started with. membership is the one
	// in effect now: the newest config entry in the log, or initialMembership if there is
	// none. membershipIndex is that entry's log index.
	initialMembership Membership
	membership        Membership
	membershipIndex   uint64

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

// NewNode creates a follower with an empty log.
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
	voters := map[NodeID]bool{}
	for _, p := range c.Peers {
		if !learners[p] {
			voters[p] = true
		}
	}
	initial := Membership{Old: voters, New: map[NodeID]bool{}}
	n := &node{
		id: c.ID, peers: append([]NodeID(nil), c.Peers...), initialMembership: initial,
		log: newLog(), electionTick: c.ElectionTick, heartbeatTick: c.HeartbeatTick,
		next: map[NodeID]uint64{}, match: map[NodeID]uint64{},
	}
	n.recomputeMembership()
	return n, nil
}

// Tick advances the clocks. A leader sends heartbeats; a voter that has heard nothing
// from a leader for too long starts a pre-vote.
func (n *node) Tick() {
	n.electionElapsed++
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTick {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
	}
	// Learners and removed nodes never start elections: they could not win one.
	if n.electionElapsed >= n.electionTick && n.role != Leader && n.isVoter(n.id) {
		n.electionElapsed = 0
		n.startPreVote()
	}
}

// Propose appends data to the log and sends it to the followers. Only the leader can.
func (n *node) Propose(data []byte) error {
	if n.role != Leader {
		return errors.New("raft: proposal to non-leader")
	}
	return n.proposeInternal(data)
}

// proposeInternal is the shared append path. Config changes go through it too, so they
// are replicated and committed like any other entry.
func (n *node) proposeInternal(data []byte) error {
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Data: data}
	if err := n.log.append([]Entry{e}); err != nil {
		return err
	}
	// A new config entry takes effect as soon as it is in the log, before it commits.
	// Only config entries can change membership, so there is no need to rescan the log
	// for ordinary data.
	if _, ok := decodeConfChange(data); ok {
		n.recomputeMembership()
	}
	n.unstable = append(n.unstable, e)
	n.match[n.id] = n.log.lastIndex()
	n.broadcastAppend()
	return nil
}

// Step handles one incoming message.
func (n *node) Step(m Message) error {
	if m.To != 0 && m.To != n.id {
		return errors.New("raft: wrong destination")
	}
	// A higher term makes us a follower. Pre-vote messages are the exception: they carry a
	// term that has not happened yet, and adopting it would defeat the point of pre-vote.
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

// Ready collects everything the caller still has to save, send and apply.
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

// Advance marks the last Ready as handled.
func (n *node) Advance() {
	n.unstable = nil
	n.msgs = nil
	n.log.applied = n.log.committed
	n.lastHard = HardState{Term: n.term, Vote: n.vote, Commit: n.log.committed}
}

func (n *node) Status() (Role, uint64) { return n.role, n.term }
func (n *node) Leader() NodeID         { return NodeID(n.leader) }
func (n *node) ConfState() ConfState   { return confStateFromMembership(n.membership) }
func (n *node) send(m Message)         { m.From = n.id; n.msgs = append(n.msgs, m) }

// becomeFollower steps down (or stays a follower) in the given term.
func (n *node) becomeFollower(term uint64, leader NodeID) {
	n.role = Follower
	n.term = term
	n.vote = 0
	n.leader = uint64(leader)
	n.votes = nil
	n.preVote = false
	n.electionElapsed = 0
}

// becomeLeader starts leading: it begins replicating from the end of its own log.
func (n *node) becomeLeader() {
	n.role = Leader
	n.leader = uint64(n.id)
	n.next = map[NodeID]uint64{}
	n.match = map[NodeID]uint64{n.id: n.log.lastIndex()}
	n.preVote = false
	for _, p := range n.peers {
		n.next[p] = n.log.lastIndex() + 1
	}
	// If a joint config entry from a previous leader is still in the log, Raft will not
	// commit it by counting replies alone, because it is from an old term. Appending an
	// empty entry in our own term lets it commit without waiting for a client write.
	if n.membership.Joint {
		_ = n.proposeInternal(append([]byte(nil), jointLeaderNoop...))
		return
	}
	n.broadcastAppend()
}

// broadcastAppend sends every other node whatever log entries it is missing.
func (n *node) broadcastAppend() {
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
}

// sendAppend sends one node the entries after the last one we think it has.
func (n *node) sendAppend(to NodeID) {
	next := n.next[to]
	prev := next - 1
	pt, _ := n.log.term(prev)
	n.send(Message{Type: MsgAppend, To: to, Term: n.term, Index: prev, LogTerm: pt, Commit: n.log.committed, Entries: n.log.entriesFrom(next)})
}

// handleAppend handles log entries (or a heartbeat) from the leader.
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
		// Only a non-empty batch can change the log, so only then can membership change.
		n.recomputeMembership()
		n.unstable = append(n.unstable, cloneEntries(m.Entries)...)
	}
	if m.Commit > n.log.committed {
		n.log.committed = min(m.Commit, n.log.lastIndex())
	}
	n.send(Message{Type: MsgAppendResp, To: m.From, Term: n.term, Index: n.log.lastIndex()})
}

// handleAppendResp updates what we know a follower has, and commits what a quorum has.
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

// advanceCommit commits the highest entry that a quorum of voters has, if it is from the
// current term (an older-term entry is only committed along with a newer one).
//
// Learners are not counted. While a membership change is joint, the quorum needs a
// majority of both the old and new voters (see Membership). That check is not a simple
// "n-th highest match index", so each candidate index is tried from the highest down.
func (n *node) advanceCommit() {
	candidates := map[uint64]bool{n.log.lastIndex(): true}
	for _, m := range n.match {
		candidates[m] = true
	}
	sorted := make([]uint64, 0, len(candidates))
	for c := range candidates {
		sorted = append(sorted, c)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] > sorted[j] })
	for _, candidate := range sorted {
		if candidate <= n.log.committed {
			break
		}
		term, _ := n.log.term(candidate)
		if term != n.term {
			continue
		}
		acked := map[NodeID]bool{}
		for p, matchIndex := range n.match {
			if matchIndex >= candidate {
				acked[p] = true
			}
		}
		if n.membership.HasQuorum(acked) {
			n.log.committed = candidate
			n.broadcastAppend()
			n.maybeLeaveJointOrStepDown()
			return
		}
	}
}

// maybeLeaveJointOrStepDown runs after the commit index moves. It does two things:
//  1. Once the joint config entry has committed, the leader proposes the second entry
//     that finishes the change.
//  2. If the finished change removed this leader from the voters, it steps down.
func (n *node) maybeLeaveJointOrStepDown() {
	if n.role != Leader {
		return
	}
	if n.membership.Joint && n.log.committed >= n.membershipIndex {
		_ = n.proposeLeaveJoint()
		return
	}
	if !n.membership.Joint && !n.isVoter(n.id) {
		n.becomeFollower(n.term, 0)
	}
}

// cloneEntries copies entries so later changes to the log do not affect them.
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
