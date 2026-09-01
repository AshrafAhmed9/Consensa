package raft

import "errors"

// Membership captures a fixed, joint, or final voter configuration. During Joint, a
// proposal needs a majority of both sets; using only their union permits disjoint quorums.
type Membership struct {
	Old, New map[NodeID]bool
	Joint    bool
}

// NewMembership creates an initial stable voter set.
func NewMembership(voters []NodeID) Membership {
	m := Membership{Old: map[NodeID]bool{}, New: map[NodeID]bool{}}
	for _, v := range voters {
		m.Old[v] = true
	}
	return m
}

// EnterJoint begins the two-majority transition after a learner has caught up.
func (m *Membership) EnterJoint(next []NodeID) error {
	if m.Joint {
		return errors.New("raft: membership already joint")
	}
	if len(next) == 0 {
		return errors.New("raft: empty voter set")
	}
	m.New = map[NodeID]bool{}
	for _, v := range next {
		m.New[v] = true
	}
	m.Joint = true
	return nil
}

// LeaveJoint makes the new set the sole voting configuration.
func (m *Membership) LeaveJoint() error {
	if !m.Joint {
		return errors.New("raft: membership is not joint")
	}
	m.Old = m.New
	m.New = map[NodeID]bool{}
	m.Joint = false
	return nil
}

// Voters returns every node ID that is a legitimate voter under this configuration: Old
// alone when stable, or Old union New while Joint. This is deliberately broader than
// "who can make something commit" (HasQuorum requires a majority of EACH set
// separately) -- a member of New that isn't in Old yet may still grant a vote or
// acknowledge a log entry, and its participation is exactly what a majority-of-New check
// needs to ever be satisfiable during the joint phase.
func (m Membership) Voters() map[NodeID]bool {
	out := map[NodeID]bool{}
	for id := range m.Old {
		out[id] = true
	}
	if m.Joint {
		for id := range m.New {
			out[id] = true
		}
	}
	return out
}

func majority(set map[NodeID]bool, votes map[NodeID]bool) bool {
	n := 0
	for id := range set {
		if votes[id] {
			n++
		}
	}
	return n >= len(set)/2+1
}

// HasQuorum applies the dual-majority requirement during the unsafe transition window.
func (m Membership) HasQuorum(votes map[NodeID]bool) bool {
	if !majority(m.Old, votes) {
		return false
	}
	return !m.Joint || majority(m.New, votes)
}
