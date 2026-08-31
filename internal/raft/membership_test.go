package raft

import "testing"

// TestJointMembershipRequiresBothMajorities proves union-majority votes cannot commit during transition.
func TestJointMembershipRequiresBothMajorities(t *testing.T) {
	m := NewMembership([]NodeID{1, 2, 3})
	if e := m.EnterJoint([]NodeID{3, 4, 5}); e != nil {
		t.Fatal(e)
	}
	if m.HasQuorum(map[NodeID]bool{1: true, 2: true, 4: true}) {
		t.Fatal("disjoint union majority accepted")
	}
	if !m.HasQuorum(map[NodeID]bool{1: true, 2: true, 3: true, 4: true}) {
		t.Fatal("dual majority rejected")
	}
}
