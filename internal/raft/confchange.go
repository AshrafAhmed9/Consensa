package raft

import (
	"bytes"
	"encoding/json"
	"errors"
)

// confChangePrefix marks a log entry as a raft-internal membership configuration change,
// not application data. It starts with a NUL byte, which no caller's own Entry.Data ever
// legitimately starts with -- every existing caller in this codebase encodes JSON or
// length-prefixed text beginning with a printable byte -- so this reservation cannot
// collide with any real application command sharing the same Raft group, the same
// argument kv.reservedKeyPrefix already makes for its own "raft/" namespace one layer up.
var confChangePrefix = []byte("\x00raft/confchange\x00")

// jointLeaderNoop is a Raft-internal current-term entry a newly elected leader appends
// while a joint configuration is in progress. Raft's current-term commit rule otherwise
// prevents it from committing an inherited joint entry until an unrelated client write
// happens to arrive, leaving a leader-crash recovery transition needlessly stuck.
var jointLeaderNoop = []byte("\x00raft/joint-leader-noop/v1\x00")

// confChangeEntry is the wire encoding of one membership transition step, applied the
// moment it is APPENDED to a node's own log -- not once committed. This is the specific,
// easy-to-get-wrong rule the joint-consensus algorithm depends on (Raft §6 / Ongaro's
// thesis, "Cluster membership changes"): a server must always use the LATEST
// configuration in its log, committed or not, so that leader election and commit-quorum
// decisions made mid-transition are evaluated identically by every server that has seen
// the same log, rather than only by servers that happen to have already committed it.
//
// Joint=true establishes the joint phase; Old is the configuration this entry replaces,
// captured explicitly (not re-derived from whatever came before) so recomputeMembership
// can reconstruct the correct membership purely by reading log entries in isolation, one
// at a time, in order. Joint=false finalizes New as the sole configuration and ends the
// transition -- proposeLeaveJoint (below) always sets New to the prior entry's New and
// leaves Old empty, since Old is meaningless once the transition is over.
type confChangeEntry struct {
	Old, New []NodeID
	Learners []NodeID
	Joint    bool
}

func marshalConfChange(c confChangeEntry) ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return append(append([]byte(nil), confChangePrefix...), data...), nil
}

func decodeConfChange(data []byte) (confChangeEntry, bool) {
	if !bytes.HasPrefix(data, confChangePrefix) {
		return confChangeEntry{}, false
	}
	var c confChangeEntry
	if err := json.Unmarshal(data[len(confChangePrefix):], &c); err != nil {
		return confChangeEntry{}, false
	}
	return c, true
}

// isConfChangeData identifies the reserved Raft-internal command family. Host must not
// deliver these entries to an application state machine: membership changes affect Raft
// itself and an application decoder (for example the ANN mutation decoder) has no valid
// interpretation for them.
func isConfChangeData(data []byte) bool { return bytes.HasPrefix(data, confChangePrefix) }

func isRaftInternalData(data []byte) bool {
	return isConfChangeData(data) || bytes.Equal(data, jointLeaderNoop)
}

func membershipFromConfChange(c confChangeEntry) Membership {
	m := Membership{Old: map[NodeID]bool{}, New: map[NodeID]bool{}, Joint: c.Joint}
	for _, id := range c.Old {
		m.Old[id] = true
	}
	for _, id := range c.New {
		m.New[id] = true
	}
	if !c.Joint {
		// A finalized entry's New becomes the sole active configuration -- mirrors
		// Membership.LeaveJoint's own Old = New assignment, kept consistent here since
		// recomputeMembership rebuilds Membership directly from log entries rather than
		// by calling LeaveJoint on a live Membership value.
		m.Old = m.New
		m.New = map[NodeID]bool{}
	}
	return m
}

func membershipFromConfState(c ConfState) Membership {
	m := Membership{Old: map[NodeID]bool{}, New: map[NodeID]bool{}, Joint: c.Joint}
	for _, id := range c.Old {
		m.Old[id] = true
	}
	for _, id := range c.New {
		m.New[id] = true
	}
	return m
}

func confStateFromMembership(m Membership) ConfState {
	c := ConfState{Joint: m.Joint}
	for id := range m.Old {
		c.Old = append(c.Old, id)
	}
	for id := range m.New {
		c.New = append(c.New, id)
	}
	return c
}

// recomputeMembership rebuilds n.membership and n.membershipIndex by scanning the
// CURRENT log for the latest confChangeEntry, from scratch, every time it's called.
// Deliberately not incremental: incrementally patching membership on every append would
// need separate, carefully-proven logic for the truncation/conflict-resolution path in
// raftLog.append (where entries -- possibly including a config-change entry this node
// already applied -- get silently discarded and replaced), and getting that wrong would
// silently corrupt quorum computation, the single worst class of bug this project can
// have. A full rescan is O(log length) and this codebase's own priorities already accept
// worse for correctness-first code elsewhere (storage.DB.Get's linear scan,
// ReadIndex's per-read log entry) -- this is the same trade, applied to the one place
// where getting it wrong is least acceptable of all.
func (n *node) recomputeMembership() {
	membership := n.initialMembership
	membershipIndex := uint64(0)
	for _, e := range n.log.entries {
		if c, ok := decodeConfChange(e.Data); ok {
			membership = membershipFromConfChange(c)
			membershipIndex = e.Index
		}
	}
	n.membership = membership
	n.membershipIndex = membershipIndex
}

func (n *node) restoreConfState(c ConfState, index uint64) {
	if len(c.Old) == 0 { // legacy snapshot: its startup config is the only available state.
		return
	}
	n.initialMembership = membershipFromConfState(c)
	n.membership = n.initialMembership
	n.membershipIndex = index
}

// isVoter reports whether id is currently a legitimate voter -- a member of Membership's
// Voters() union, which includes both Old and (while Joint) New.
func (n *node) isVoter(id NodeID) bool { return n.membership.Voters()[id] }

// votingPeers returns every currently-eligible voter except self, for messaging
// PreVote/Vote requests to -- during the joint phase this must be Old ∪ New, not just
// Old, or a majority of New (required by HasQuorum) could never be reachable at all.
func (n *node) votingPeers() []NodeID {
	voters := n.membership.Voters()
	out := make([]NodeID, 0, len(voters))
	for id := range voters {
		if id != n.id {
			out = append(out, id)
		}
	}
	return out
}

// ProposeConfChange begins a membership transition to newVoters (with newLearners as the
// accompanying non-voting set), entering the joint phase immediately on this leader's own
// log -- see confChangeEntry's own doc comment for why "on append," not "on commit,"
// is the rule that makes this safe. Only the leader may propose one, and only when no
// transition is already in progress (mirroring Membership.EnterJoint's own guard): a
// second concurrent transition would make "Old" and "New" ambiguous, which is exactly the
// kind of case this project's own engineering rules say to reject outright rather than
// guess at a resolution for.
//
// Every ID in newVoters and newLearners must already be a member of n.peers, the static,
// transport-reachable universe this node was constructed with (see NewNode) -- adding a
// genuinely new, previously-unknown node still requires the transport layer (raft.Host)
// to learn its network address first, which is a separate concern this package
// deliberately knows nothing about (see transport.go's own doc comment on that boundary).
// Promoting an existing learner, or changing which of the already-known peers are
// voters, is exactly what this closes; growing the cluster to a brand-new physical node
// is not.
func (n *node) ProposeConfChange(newVoters, newLearners []NodeID) error {
	if n.role != Leader {
		return errors.New("raft: confChange proposal to non-leader")
	}
	if n.membership.Joint {
		return errors.New("raft: membership change already in progress")
	}
	if len(newVoters) == 0 {
		return errors.New("raft: empty voter set")
	}
	known := map[NodeID]bool{}
	for _, p := range n.peers {
		known[p] = true
	}
	for _, id := range append(append([]NodeID(nil), newVoters...), newLearners...) {
		if !known[id] {
			return errors.New("raft: confChange target is not a known peer")
		}
	}
	voters := map[NodeID]bool{}
	for _, id := range newVoters {
		voters[id] = true
	}
	for _, id := range newLearners {
		if voters[id] {
			return errors.New("raft: confChange voter cannot also be a learner")
		}
	}
	old := make([]NodeID, 0, len(n.membership.Old))
	for id := range n.membership.Old {
		old = append(old, id)
	}
	data, err := marshalConfChange(confChangeEntry{Old: old, New: append([]NodeID(nil), newVoters...), Learners: append([]NodeID(nil), newLearners...), Joint: true})
	if err != nil {
		return err
	}
	return n.proposeInternal(data)
}

// proposeLeaveJoint appends the entry that finalizes the current joint transition's New
// set as the sole configuration. Called automatically (see advanceCommit in node.go) once
// this leader observes the joint entry itself has committed -- a caller never calls this
// directly, matching the paper's own two-step protocol: the leader alone decides when
// C_old,new is safely committed and only then proposes C_new, never both entries at once.
func (n *node) proposeLeaveJoint() error {
	newVoters := make([]NodeID, 0, len(n.membership.New))
	for id := range n.membership.New {
		newVoters = append(newVoters, id)
	}
	data, err := marshalConfChange(confChangeEntry{New: newVoters, Joint: false})
	if err != nil {
		return err
	}
	return n.proposeInternal(data)
}
