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
	// ProposeConfChange begins a membership transition to newVoters/newLearners via the
	// joint-consensus protocol (see confchange.go's own doc comments for the full
	// safety argument). Only the leader may call it, and only when no transition is
	// already in progress; the leave-joint follow-up that finalizes the transition is
	// automatic once the joint entry itself commits, never a second caller-driven call.
	ProposeConfChange(newVoters, newLearners []NodeID) error
	Ready() Ready
	Advance()
	// Status reports this replica's own view of its role and term, for administrative
	// and diagnostic surfaces. It is read-only and never changes protocol behavior.
	Status() (Role, uint64)
	// ConfState returns the membership that must accompany any snapshot created from this
	// replica. A snapshot without it cannot safely replace the configuration entries in
	// the compacted log prefix.
	ConfState() ConfState
}
type node struct {
	id    NodeID
	peers []NodeID

	// initialMembership is the voter/learner configuration NewNode was constructed with,
	// used as recomputeMembership's base case when no confChangeEntry exists in the log
	// yet. membership/membershipIndex are recomputed from the log itself after every
	// append (see recomputeMembership's own doc comment for why this is a full rescan
	// rather than incremental state).
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

func (n *node) Tick() {
	n.electionElapsed++
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTick {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
	}
	// A non-voter (a learner, or a node this transition has removed) never starts an
	// election, no matter how long it goes without hearing from a leader: it could never
	// actually win one (no real voter would count its vote request toward HasQuorum --
	// see startPreVote/startElection below, which only message n.votingPeers()), and
	// letting it try would just be wasted messages on every election timeout for as long
	// as it stays outside the voting configuration.
	if n.electionElapsed >= n.electionTick && n.role != Leader && n.isVoter(n.id) {
		n.electionElapsed = 0
		n.startPreVote()
	}
}
func (n *node) Propose(data []byte) error {
	if n.role != Leader {
		return errors.New("raft: proposal to non-leader")
	}
	return n.proposeInternal(data)
}

// proposeInternal is Propose's shared implementation, also used by ProposeConfChange and
// proposeLeaveJoint (confchange.go) to append a raft-internal configuration entry through
// the identical log-append/broadcast path ordinary application data uses -- a
// configuration change is replicated and made safe by exactly the same mechanism as any
// other entry, which is the whole point of representing it as a log entry rather than as
// some separate out-of-band protocol.
func (n *node) proposeInternal(data []byte) error {
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Data: data}
	if err := n.log.append([]Entry{e}); err != nil {
		return err
	}
	// Only a config-change entry can possibly change membership, and a leader's own
	// append (always at lastIndex()+1) never truncates anything the way a follower's
	// conflict-resolving append can -- so checking just this one entry, rather than
	// rescanning the whole log, is both sufficient and correct here, not merely an
	// optimization applied where it happens to be safe. "On append, not on commit" (see
	// confChangeEntry's doc comment) is preserved: this still runs before the entry is
	// sent to anyone else. Rescanning the full log on every single Propose call --
	// including every ordinary application write, the actual hot path under load -- was
	// the real cost the CI regression this comment replaces came from.
	if _, ok := decodeConfChange(data); ok {
		n.recomputeMembership()
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
func (n *node) ConfState() ConfState   { return confStateFromMembership(n.membership) }
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
	// A joint entry inherited from a crashed leader can be from an older term. The Raft
	// current-term commit rule correctly refuses to commit it on acknowledgements alone;
	// append an internal no-op now so the new leader can establish a current-term commit
	// under the dual-majority rule without waiting for an unrelated client proposal.
	if n.membership.Joint {
		_ = n.proposeInternal(append([]byte(nil), jointLeaderNoop...))
		return
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
		// raftLog.append only ever truncates or extends the log as a direct result of
		// appending a non-empty entry batch (see log.go) -- a bare heartbeat (empty
		// Entries) changes nothing about what's in the log, so it cannot possibly change
		// what recomputeMembership would find, and calling it on every single heartbeat
		// tick regardless is a real O(log length) cost paid for nothing. Found as an
		// actual CI failure under sustained load (a long-running e2e test's leadership
		// went unstable, "proposal to non-leader"), not by inspection -- this call used
		// to run unconditionally here.
		n.recomputeMembership()
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
	//
	// During the joint phase this must be a majority of Old AND a majority of New
	// separately (Membership.HasQuorum), not a majority of their union -- the entire
	// reason joint consensus exists is that a union-based majority could be satisfied by
	// two DISJOINT sets of servers, one under the old configuration and one under the
	// new, each of which could independently elect an incompatible leader. Candidate
	// commit indices are checked from the highest down, since HasQuorum's dual
	// requirement isn't monotonic in a single sorted array the way a simple majority's
	// "n-th highest match" trick assumes.
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

// maybeLeaveJointOrStepDown runs after every successful commit advance and closes two
// things joint consensus needs beyond plain quorum safety: (1) once this leader observes
// the joint (C_old,new) entry itself has committed, it automatically proposes the
// finalizing entry -- the paper's own two-step protocol, never both entries at once, and
// never a caller's job to sequence; recomputeMembership flips n.membership.Joint to false
// the instant this call appends that entry, so a repeated call here is naturally a no-op
// once it's done. (2) once membership is no longer joint and this node is no longer a
// voter at all (it was removed), a leader steps down rather than continuing to act as
// leader for a configuration it isn't even a member of.
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
