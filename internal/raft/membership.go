package raft

// Membership is the set of voters that decide elections and commits.
//
// Normally only Old is used. While the group is changing voters ("joint" mode), a vote
// or commit needs a majority of Old AND a majority of New. Counting a majority of the
// union instead would let two disjoint groups each win, which is the bug joint
// consensus exists to prevent.
type Membership struct {
	Old, New map[NodeID]bool
	Joint    bool
}

// Voters returns every node that may vote right now: Old, plus New while joint.
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

// majority reports whether more than half of set appears in votes.
func majority(set map[NodeID]bool, votes map[NodeID]bool) bool {
	n := 0
	for id := range set {
		if votes[id] {
			n++
		}
	}
	return n >= len(set)/2+1
}

// HasQuorum reports whether votes are enough: a majority of Old, and while joint also a
// majority of New.
func (m Membership) HasQuorum(votes map[NodeID]bool) bool {
	if !majority(m.Old, votes) {
		return false
	}
	return !m.Joint || majority(m.New, votes)
}
