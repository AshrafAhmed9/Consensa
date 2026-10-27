package raft

import "testing"

// newLearnerGroup builds a 4-node group: 3 voters (1,2,3) and 1 learner (4), with one
// node joining as a non-voting learner.
func newLearnerGroup(t *testing.T) map[NodeID]Node {
	t.Helper()
	ns := map[NodeID]Node{}
	for _, id := range []NodeID{1, 2, 3, 4} {
		n, err := NewNode(Config{ID: id, Peers: []NodeID{1, 2, 3, 4}, Learners: []NodeID{4}, ElectionTick: 3, HeartbeatTick: 1})
		if err != nil {
			t.Fatal(err)
		}
		ns[id] = n
	}
	return ns
}

// TestLearnerNeverBecomesLeader proves a learner never starts an election, however long
// it goes without hearing from a leader.
func TestLearnerNeverBecomesLeader(t *testing.T) {
	ns := newLearnerGroup(t)
	learner := ns[4]
	for i := 0; i < 50; i++ {
		learner.Tick()
		r := learner.Ready()
		if len(r.Messages) != 0 {
			t.Fatalf("learner sent messages on its own after %d ticks with no leader contact: %+v", i, r.Messages)
		}
		if role, _ := learner.Status(); role != Follower {
			t.Fatalf("learner's role became %v after %d ticks, want it to stay Follower forever", role, i)
		}
		learner.Advance()
	}
}

// TestLearnerCannotGrantAVote proves a learner rejects vote requests. Real nodes never
// send it one, so this sends one by hand.
func TestLearnerCannotGrantAVote(t *testing.T) {
	ns := newLearnerGroup(t)
	learner := ns[4]
	if err := learner.Step(Message{Type: MsgPreVote, From: 1, To: 4, Term: 1}); err != nil {
		t.Fatal(err)
	}
	r := learner.Ready()
	if len(r.Messages) != 1 || r.Messages[0].Type != MsgPreVoteResp || !r.Messages[0].Reject {
		t.Fatalf("learner's MsgPreVote response = %+v, want exactly one rejected MsgPreVoteResp", r.Messages)
	}
	learner.Advance()

	if err := learner.Step(Message{Type: MsgVote, From: 1, To: 4, Term: 1}); err != nil {
		t.Fatal(err)
	}
	r = learner.Ready()
	if len(r.Messages) != 1 || r.Messages[0].Type != MsgVoteResp || !r.Messages[0].Reject {
		t.Fatalf("learner's MsgVote response = %+v, want exactly one rejected MsgVoteResp", r.Messages)
	}
}

// TestCommitNeverAdvancesOnLearnerAcksAlone proves an entry held only by the leader and a
// learner is not committed. That is 2 of 4 nodes, but only a majority of the 3 voters
// counts. Otherwise a write could be reported as safe while only one other voter has it.
func TestCommitNeverAdvancesOnLearnerAcksAlone(t *testing.T) {
	ns := newLearnerGroup(t)
	for i := 0; i < 3; i++ {
		ns[1].Tick()
	}
	deliver(ns)
	if role, _ := ns[1].Status(); role != Leader {
		t.Fatalf("node 1's role = %v, want Leader after a full election", role)
	}

	if err := ns[1].Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}

	// Deliver leader(1)'s outbound AppendEntries only to the learner(4); voters 2 and 3
	// never see it, simulating a leader isolated from its real quorum but still able to
	// reach a learner (e.g. the learner is co-located or on a healthier link).
	r := ns[1].Ready()
	for _, m := range r.Messages {
		if m.To == 4 {
			if err := ns[4].Step(m); err != nil {
				t.Fatal(err)
			}
		}
	}
	ns[1].Advance()

	r4 := ns[4].Ready()
	for _, m := range r4.Messages {
		if m.To == 1 {
			if err := ns[1].Step(m); err != nil {
				t.Fatal(err)
			}
		}
	}
	ns[4].Advance()

	got := ns[1].Ready().HardState.Commit
	if got != 0 {
		t.Fatalf("leader's commit index = %d after only the learner acknowledged, want 0 (unchanged) -- a learner's ack must never count toward quorum", got)
	}

	// Now let voter 2 also acknowledge -- leader(1) + voter(2) is a real majority of the
	// 3 voters, and commit must advance. The original AppendEntries queued for voter 2
	// back when Propose first broadcast was never delivered (only node 4's copy was, in
	// the block above) and Advance() already discarded it, so a heartbeat tick is needed
	// to make the leader re-send outstanding entries to every peer, voter 2 included --
	// exactly how a real leader recovers from a message that never arrived.
	ns[1].Tick()
	r = ns[1].Ready()
	for _, m := range r.Messages {
		if m.To == 2 {
			if err := ns[2].Step(m); err != nil {
				t.Fatal(err)
			}
		}
	}
	ns[1].Advance()
	r2 := ns[2].Ready()
	for _, m := range r2.Messages {
		if m.To == 1 {
			if err := ns[1].Step(m); err != nil {
				t.Fatal(err)
			}
		}
	}
	ns[2].Advance()

	got = ns[1].Ready().HardState.Commit
	if got == 0 {
		t.Fatal("leader's commit index still 0 after a real majority of voters (leader + voter 2) acknowledged")
	}
}

// TestLearnerReceivesReplicatedLog proves a learner still receives the log, so it can
// catch up and later be promoted to a voter.
func TestLearnerReceivesReplicatedLog(t *testing.T) {
	ns := newLearnerGroup(t)
	for i := 0; i < 3; i++ {
		ns[1].Tick()
	}
	deliver(ns)
	if err := ns[1].Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	deliver(ns)

	leaderCommit := ns[1].Ready().HardState.Commit
	learnerCommit := ns[4].Ready().HardState.Commit
	if leaderCommit == 0 {
		t.Fatal("leader never committed the proposal")
	}
	if learnerCommit != leaderCommit {
		t.Fatalf("learner's own committed index = %d, want it caught up to the leader's %d", learnerCommit, leaderCommit)
	}
}
