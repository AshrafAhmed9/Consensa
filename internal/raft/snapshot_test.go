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
