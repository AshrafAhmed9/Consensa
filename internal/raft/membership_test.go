package raft

import "testing"

// TestJointMembershipRequiresBothMajorities proves a majority of the union is not enough
// during a membership change: votes need a majority of the old set and of the new set.
func TestJointMembershipRequiresBothMajorities(t *testing.T) {
	m := Membership{
		Old:   map[NodeID]bool{1: true, 2: true, 3: true},
		New:   map[NodeID]bool{3: true, 4: true, 5: true},
		Joint: true,
	}
	if m.HasQuorum(map[NodeID]bool{1: true, 2: true, 4: true}) {
		t.Fatal("disjoint union majority accepted")
	}
	if !m.HasQuorum(map[NodeID]bool{1: true, 2: true, 3: true, 4: true}) {
		t.Fatal("dual majority rejected")
	}
}
