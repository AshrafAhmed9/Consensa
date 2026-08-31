package raft

import "testing"

func newTestNode(t *testing.T, id NodeID) Node {
	t.Helper()
	n, e := NewNode(Config{ID: id, Peers: []NodeID{1, 2, 3}, ElectionTick: 3, HeartbeatTick: 1})
	if e != nil {
		t.Fatal(e)
	}
	return n
}
func deliver(nodes map[NodeID]Node) {
	for again := true; again; {
		again = false
		for _, n := range nodes {
			r := n.Ready()
			for _, m := range r.Messages {
				if target := nodes[m.To]; target != nil {
					if e := target.Step(m); e != nil {
						panic(e)
					}
					again = true
				}
			}
			n.Advance()
		}
	}
}

// TestLeaderElectionAndReplication proves that a majority election is sufficient to
// replicate and commit a command; no wall clock or goroutine participates in the test.
func TestLeaderElectionAndReplication(t *testing.T) {
	ns := map[NodeID]Node{1: newTestNode(t, 1), 2: newTestNode(t, 2), 3: newTestNode(t, 3)}
	for i := 0; i < 3; i++ {
		ns[1].Tick()
	}
	deliver(ns)
	if e := ns[1].Propose([]byte("x")); e != nil {
		t.Fatal(e)
	}
	deliver(ns)
	for id, n := range ns {
		state := n.(*node)
		if state.log.committed != 1 {
			t.Fatalf("node %d did not commit proposal", id)
		}
	}
}

// TestFigure8CommitRule proves a leader cannot commit an old-term entry solely because
// replicas have it. The current-term check is Raft §5.4.2's protection against Figure 8.
func TestFigure8CommitRule(t *testing.T) {
	n := newTestNode(t, 1).(*node)
	n.role = Leader
	n.term = 2
	n.log.entries = []Entry{{}, {Index: 1, Term: 1, Data: []byte("old")}}
	n.match = map[NodeID]uint64{1: 1, 2: 1, 3: 0}
	n.advanceCommit()
	if n.log.committed != 0 {
		t.Fatalf("prior-term entry committed: %d", n.log.committed)
	}
}

// TestDelayedPreVoteResponseCannotRestartLeader proves pre-vote replies belong only to
// an active pre-vote round. Without that guard, a delayed response can start an election
// after leadership is established and needlessly disrupt a healthy cluster.
func TestDelayedPreVoteResponseCannotRestartLeader(t *testing.T) {
	n := newTestNode(t, 1).(*node)
	n.role = Leader
	n.term = 4
	n.votes = map[NodeID]bool{1: true}
	if err := n.Step(Message{Type: MsgPreVoteResp, From: 2, To: 1, Term: 4}); err != nil {
		t.Fatal(err)
	}
	if n.role != Leader || n.term != 4 {
		t.Fatalf("delayed pre-vote changed leader to role=%v term=%d", n.role, n.term)
	}
}
