package raft

import "testing"

// TestSnapshotThenAppend proves that log-index arithmetic remains correct after a
// snapshot discards a prefix; future entries retain their original absolute indices.
func TestSnapshotThenAppend(t *testing.T) {
	n := newTestNode(t, 1).(*node)
	if err := n.Step(Message{Type: MsgSnapshot, From: 2, To: 1, Term: 1, Snapshot: Snapshot{Index: 10, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := n.Step(Message{Type: MsgAppend, From: 2, To: 1, Term: 1, Index: 10, LogTerm: 1, Entries: []Entry{{Index: 11, Term: 1, Data: []byte("next")}}}); err != nil {
		t.Fatal(err)
	}
	if got := n.log.entriesFrom(11); len(got) != 1 || got[0].Index != 11 {
		t.Fatalf("entries after snapshot = %#v", got)
	}
}

// TestSnapshotRestoresMembership proves a snapshot carries the configuration whose log
// entry it compacted. Without ConfState, this node would revert to its startup three-voter
// configuration and could use an unsafe quorum after receiving a post-change snapshot.
func TestSnapshotRestoresMembership(t *testing.T) {
	n := newTestNode(t, 1).(*node)
	s := Snapshot{Index: 10, Term: 4, ConfState: ConfState{Old: []NodeID{2, 3, 4}, New: []NodeID{2, 3, 4, 5}, Joint: true}}
	if err := n.Step(Message{Type: MsgSnapshot, From: 2, To: 1, Term: 4, Snapshot: s}); err != nil {
		t.Fatal(err)
	}
	if !n.membership.Joint || n.isVoter(1) || !n.isVoter(5) {
		t.Fatalf("membership after snapshot = %#v, want joint {2,3,4}->{2,3,4,5}", n.membership)
	}
	if n.membershipIndex != 10 {
		t.Fatalf("membership index = %d, want snapshot index 10", n.membershipIndex)
	}
}

func TestConfStateReportsCurrentMembership(t *testing.T) {
	n := newTestNode(t, 1).(*node)
	n.membership = Membership{Old: map[NodeID]bool{1: true, 2: true}, New: map[NodeID]bool{2: true, 3: true}, Joint: true}
	c := n.ConfState()
	if !c.Joint || len(c.Old) != 2 || len(c.New) != 2 {
		t.Fatalf("ConfState() = %#v, want current joint membership", c)
	}
}
