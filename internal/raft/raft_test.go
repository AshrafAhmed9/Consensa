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

// TestFigure8UnsafeCommitWouldBeOverwritten is the full, multi-node version of
// TestFigure8CommitRule's single-node proof: it drives the exact 5-server scenario from
// Raft §5.4.2 / Figure 8 through real Step() calls -- real vote-freshness checks, real
// AppendEntries accept/reject/truncate, and the real advanceCommit term guard -- and
// shows that an entry replicated to a genuine majority of the cluster (3 of 5 nodes) is
// still silently overwritten by a later leader, because it was never anchored by an
// entry from that leader's own term. Only the initial log/term states are hand-set
// (matching TestFigure8CommitRule and TestDelayedPreVoteResponseCannotRestartLeader
// above), standing in for elections this test isn't about; every safety-relevant
// decision after that runs through the unmodified production code path.
//
// Correspondence with the paper's four-panel figure:
//   - n1 = S1: leader at term 2, appends "b" at index 2, replicates to n2 only, crashes.
//   - n2 = S2: has "b". Wins a real election at term 3 (its log is fresher than n3/n4's),
//     replicates "b" to n3 and n4 -- 3 of 5 nodes now have it, a genuine majority -- then
//     crashes before ever committing an entry of its own term 3.
//   - n5 = S5: modeling a separate, even briefer earlier leadership (term 4) that
//     appended its own conflicting entry "z" at index 2 but crashed before replicating it
//     anywhere -- exactly why it starts pre-set rather than elected on-scene here.
//   - n5 then wins a real election over n3/n4 (its log's last entry has the higher term,
//     which the freshness rule accepts as "more up to date" independent of wall-clock
//     history), and overwrites "b" with "z" via a real, unmodified AppendEntries call.
func TestFigure8UnsafeCommitWouldBeOverwritten(t *testing.T) {
	peers := []NodeID{1, 2, 3, 4, 5}
	newN := func(id NodeID) *node {
		n, err := NewNode(Config{ID: id, Peers: peers, ElectionTick: 3, HeartbeatTick: 1})
		if err != nil {
			t.Fatal(err)
		}
		return n.(*node)
	}
	n1, n2, n3, n4, n5 := newN(1), newN(2), newN(3), newN(4), newN(5)

	base := Entry{Index: 1, Term: 1, Data: []byte("a")}
	for _, n := range []*node{n1, n2, n3, n4, n5} {
		n.log.entries = []Entry{{}, base}
		n.log.committed = 1
	}

	// n1: leader at term 2, appends "b" at index 2, replicates to n2 only.
	n1.role, n1.term = Leader, 2
	unsafeEntry := Entry{Index: 2, Term: 2, Data: []byte("b")}
	n1.log.entries = append(n1.log.entries, unsafeEntry)
	if err := n2.Step(Message{Type: MsgAppend, From: 1, To: 2, Term: 2, Index: 1, LogTerm: 1, Commit: 1, Entries: []Entry{unsafeEntry}}); err != nil {
		t.Fatal(err)
	}
	n2.Advance()
	// n1 crashes here and takes no further part.

	// n5: stands in for its own earlier, even briefer leadership (term 4) that appended
	// a conflicting entry and crashed before replicating it to anyone -- see the comment
	// above for why this is set up directly rather than played out as a third election.
	n5.term = 4
	n5.log.entries = []Entry{{}, base, {Index: 2, Term: 4, Data: []byte("z")}}

	// n2 wins a real election at term 3. n3/n4's logs (index1:term1) are strictly less
	// fresh than n2's (index2:term2), so handleVote must grant. n5 stays out of contact
	// for this phase and the next -- it is modeling a node that has been separately
	// partitioned away this whole time (a fresher-but-unreplicated raw term of its own
	// is enough to unconditionally reset any candidate that hears from it at all, even
	// via a rejection, which is real Raft behavior and exactly why it must stay isolated
	// until the scenario is ready for it to reappear).
	n2.startElection()
	active := map[NodeID]Node{2: n2, 3: n3, 4: n4}
	deliver(active)
	if n2.role != Leader || n2.term != 3 {
		t.Fatalf("n2 did not win the election: role=%v term=%d", n2.role, n2.term)
	}

	// n2 replicates "b" to n3 and n4 specifically (not n5, whose log conflicts at index
	// 2), reaching match counts of n2=2, n3=2, n4=2 -- a genuine majority of the 5-node
	// cluster (n1 and n5 unconfirmed count as 0 each).
	replicate := map[NodeID]Node{2: n2, 3: n3, 4: n4}
	deliver(replicate)
	if got := []uint64{n2.match[2], n2.match[3], n2.match[4]}; got[0] != 2 || got[1] != 2 || got[2] != 2 {
		t.Fatalf("expected n2, n3, n4 all matched at index 2, got %v", got)
	}

	// The naive rule this guard exists to forbid: majority-replicated, so "committed".
	matched := 0
	for _, id := range peers {
		if n2.match[id] >= 2 {
			matched++
		}
	}
	if matched < len(n2.membership.Old)/2+1 {
		t.Fatalf("test setup is wrong: expected a real majority to have index 2, only %d/%d do", matched, len(n2.membership.Old)/2+1)
	}

	// The real guard: entry 2 is on a majority, but its term (2) isn't n2's own term
	// (3), so the unmodified advanceCommit must refuse to commit it.
	if n2.log.committed != 1 {
		t.Fatalf("advanceCommit committed a prior-term entry despite the Figure-8 guard: committed=%d", n2.log.committed)
	}
	// n2 crashes here, never having committed anything from term 3.

	// n5 campaigns for real. Its log's last entry (index2:term4) beats n3/n4's
	// (index2:term2) under the real freshness rule, so they grant it their votes even
	// though they, not n5, hold the more-replicated data.
	n5.startElection()
	remaining := map[NodeID]Node{3: n3, 4: n4, 5: n5}
	deliver(remaining)
	if n5.role != Leader {
		t.Fatalf("n5 did not win the second election: role=%v term=%d", n5.role, n5.term)
	}

	// The observable violation: "b", which was on a real majority of the cluster, is
	// gone from n3's log -- silently overwritten by n5's "z" through a real,
	// unmodified AppendEntries call performing a real conflict-truncation.
	got, ok := n3.log.term(2)
	if !ok || got != 4 {
		t.Fatalf("expected n3's index 2 to have been overwritten to term 4, got term=%d ok=%v", got, ok)
	}
	if string(n3.log.entries[2].Data) != "z" {
		t.Fatalf("expected n3 index 2 to now read %q, got %q -- \"b\" survived, contradicting the scenario", "z", n3.log.entries[2].Data)
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
