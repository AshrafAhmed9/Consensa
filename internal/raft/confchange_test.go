package raft

import "testing"

// newConfChangeGroup constructs every node with the FULL peer universe ids (so any of
// them can later be a valid ProposeConfChange target -- see its own doc comment on why a
// target must already be a known peer), but only voters actually participate in quorum
// from construction; everyone else starts as a learner, matching the realistic scenario
// ProposeConfChange exists for: promoting an already-known-but-not-yet-voting replica.
func newConfChangeGroup(t *testing.T, ids []NodeID, voters []NodeID) map[NodeID]*node {
	t.Helper()
	voterSet := map[NodeID]bool{}
	for _, v := range voters {
		voterSet[v] = true
	}
	var learners []NodeID
	for _, id := range ids {
		if !voterSet[id] {
			learners = append(learners, id)
		}
	}
	out := map[NodeID]*node{}
	for _, id := range ids {
		n, err := NewNode(Config{ID: id, Peers: ids, Learners: learners, ElectionTick: 3, HeartbeatTick: 1})
		if err != nil {
			t.Fatal(err)
		}
		out[id] = n.(*node)
	}
	return out
}

func nodesAsInterface(m map[NodeID]*node) map[NodeID]Node {
	out := make(map[NodeID]Node, len(m))
	for id, n := range m {
		out[id] = n
	}
	return out
}

// deliverConfChangeFiltered is the deterministic harness' partition switch: messages
// for which allow returns false are dropped, exactly like an unavailable network link.
func deliverConfChangeFiltered(nodes map[NodeID]Node, allow func(Message) bool) {
	for again := true; again; {
		again = false
		for _, n := range nodes {
			r := n.Ready()
			for _, m := range r.Messages {
				if allow(m) {
					if target := nodes[m.To]; target != nil {
						if err := target.Step(m); err != nil {
							panic(err)
						}
						again = true
					}
				}
			}
			n.Advance()
		}
	}
}

// TestConfChangeAppliesOnAppendNotOnCommit proves the specific rule confChangeEntry's own
// doc comment names as the one that makes joint consensus safe: a proposing leader's own
// membership view changes the instant it appends the entry to ITS OWN log, before the
// entry has reached any other node, let alone committed. Getting this backwards (waiting
// for commit) is a real, documented way to implement joint consensus incorrectly -- it
// would mean the leader itself evaluates later quorum decisions (including whether the
// joint entry itself can commit) against the OLD configuration only, which can silently
// reintroduce the disjoint-majority problem this feature exists to close.
func TestConfChangeAppliesOnAppendNotOnCommit(t *testing.T) {
	// Constructed with the full 5-peer universe (matching ProposeConfChange's own
	// requirement that every transition target already be a known, transport-reachable
	// peer -- see its doc comment), but only 1, 2, 3 start as voters -- 4 and 5 start as
	// learners, so the initial 3-node election's quorum is genuinely 2 of 3, not a
	// coincidental 3 of 5.
	ns := newConfChangeGroup(t, []NodeID{1, 2, 3, 4, 5}, []NodeID{1, 2, 3})
	for i := 0; i < 3; i++ {
		ns[1].Tick()
	}
	deliver(map[NodeID]Node{1: ns[1], 2: ns[2], 3: ns[3]})
	if ns[1].role != Leader {
		t.Fatalf("node 1's role = %v, want Leader", ns[1].role)
	}

	if err := ns[1].ProposeConfChange([]NodeID{1, 2, 3, 4, 5}, nil); err != nil {
		t.Fatal(err)
	}

	if !ns[1].membership.Joint {
		t.Fatal("leader's own membership did not become joint immediately on proposing, before any delivery")
	}
	if len(ns[1].membership.New) != 5 {
		t.Fatalf("leader's joint New set = %v, want the proposed 5-member set", ns[1].membership.New)
	}
	// Nodes 2 and 3 have not received anything yet -- their view must be unaffected.
	if ns[2].membership.Joint || ns[3].membership.Joint {
		t.Fatal("a node that has not yet received the conf-change entry already sees joint membership")
	}
}

// TestJointConfigRejectsDisjointMajorities is this feature's Figure-8-equivalent proof:
// the specific failure mode joint consensus exists to prevent must actually be prevented,
// not just modeled correctly by Membership.HasQuorum in isolation (already proven by
// TestJointMembershipRequiresBothMajorities in membership_test.go). Five nodes are
// manually set to an established joint configuration -- Old={1,2,3}, New={3,4,5}, sharing
// only node 3 -- mirroring exactly the disjoint-except-for-one-node case the naive
// "just take a majority of the union" rule would wrongly accept.
func TestJointConfigRejectsDisjointMajorities(t *testing.T) {
	ids := []NodeID{1, 2, 3, 4, 5}
	ns := newConfChangeGroup(t, ids, ids) // initial voter set is irrelevant: membership is overwritten below

	joint := confChangeEntry{Old: []NodeID{1, 2, 3}, New: []NodeID{3, 4, 5}, Joint: true}
	data, err := marshalConfChange(joint)
	if err != nil {
		t.Fatal(err)
	}
	entry := Entry{Index: 2, Term: 1, Data: data}
	for _, n := range ns {
		n.log.entries = append(n.log.entries, entry)
		n.log.committed = 1
		n.recomputeMembership()
		if !n.membership.Joint {
			t.Fatalf("node %d: membership did not become joint after the entry was manually appended to its log", n.id)
		}
	}

	// An old-config-only majority (1 and 2 -- 2 of Old's 3) must NOT be able to elect a
	// leader: it has zero overlap with New, so it can never satisfy New's own majority
	// requirement, no matter how thoroughly it satisfies Old's.
	ns[1].role, ns[1].term = Candidate, 2
	ns[1].vote = uint64(ns[1].id)
	ns[1].votes = map[NodeID]bool{1: true, 2: true}
	if ns[1].membership.HasQuorum(ns[1].votes) {
		t.Fatal("an old-config-only majority (1,2) satisfied HasQuorum -- disjoint-majority election safety broken")
	}

	// Symmetrically, a new-config-only majority (4 and 5 -- 2 of New's 3) must NOT be
	// able to elect a leader either, for the identical reason in the other direction.
	ns[4].role, ns[4].term = Candidate, 2
	ns[4].vote = uint64(ns[4].id)
	ns[4].votes = map[NodeID]bool{4: true, 5: true}
	if ns[4].membership.HasQuorum(ns[4].votes) {
		t.Fatal("a new-config-only majority (4,5) satisfied HasQuorum -- disjoint-majority election safety broken")
	}

	// A genuinely overlapping quorum -- a majority of Old (1,2,3) AND a majority of New
	// (3,4) achieved together via votes from {1,2,3,4} -- must succeed. This is what
	// joint consensus actually requires during the transition: real cross-configuration
	// agreement, not merely "enough votes from somewhere."
	ns[1].votes = map[NodeID]bool{1: true, 2: true, 3: true, 4: true}
	if !ns[1].membership.HasQuorum(ns[1].votes) {
		t.Fatal("a real dual-majority vote set (1,2,3,4) failed HasQuorum -- the check is too strict, not just too permissive")
	}
}

// TestJointTransitionCompletesAndFinalizesAutomatically proves the whole live protocol
// end to end against a real 3-node group over the deterministic in-package deliver()
// harness: proposing a transition to a 5-node voter set eventually results in every
// surviving node holding the identical FINAL (non-joint) membership, reached without any
// caller proposing the leave-joint entry itself -- maybeLeaveJointOrStepDown must do it
// automatically once the joint entry is observed committed.
func TestJointTransitionCompletesAndFinalizesAutomatically(t *testing.T) {
	ids := []NodeID{1, 2, 3, 4, 5}
	// 1, 2, 3 start as the real, active voters; 4 and 5 start as learners -- already
	// known peers (so ProposeConfChange can legally target them) and already receiving
	// replication once delivery includes them, but not counted toward quorum until
	// promoted. This is the realistic scenario: two already-running learner processes
	// being promoted to full voters, not two brand-new nodes appearing from nothing.
	ns := newConfChangeGroup(t, ids, []NodeID{1, 2, 3})
	nodes := nodesAsInterface(ns)

	for i := 0; i < 3; i++ {
		ns[1].Tick()
	}
	deliver(nodes)
	if ns[1].role != Leader {
		t.Fatalf("node 1 never became leader: role=%v", ns[1].role)
	}

	if err := ns[1].ProposeConfChange(ids, nil); err != nil {
		t.Fatal(err)
	}
	if !ns[1].membership.Joint {
		t.Fatal("leader's membership did not enter joint phase")
	}

	deliver(nodes)

	for _, id := range ids {
		if ns[id].membership.Joint {
			t.Fatalf("node %d is still in the joint phase after full delivery -- leave-joint never finalized", id)
		}
		if len(ns[id].membership.Old) != len(ids) {
			t.Fatalf("node %d's final voter set has %d members, want %d", id, len(ns[id].membership.Old), len(ids))
		}
		if !ns[id].isVoter(4) || !ns[id].isVoter(5) {
			t.Fatalf("node %d: promoted learners 4/5 are not counted as voters in the final membership", id)
		}
	}

	// The group must still work correctly after the transition: propose and commit a
	// real entry using the new 5-node configuration.
	if err := ns[1].Propose([]byte("post-transition")); err != nil {
		t.Fatal(err)
	}
	deliver(nodes)
	if ns[1].log.committed < 3 {
		t.Fatalf("post-transition commit never advanced: committed=%d", ns[1].log.committed)
	}
}

// TestRemovedLeaderStepsDown proves a leader that removes itself from the voter set
// steps down once the transition finalizes, rather than continuing to act as leader for
// a configuration it is no longer a member of.
func TestRemovedLeaderStepsDown(t *testing.T) {
	ids := []NodeID{1, 2, 3}
	ns := newConfChangeGroup(t, ids, ids)
	nodes := nodesAsInterface(ns)
	for i := 0; i < 3; i++ {
		ns[1].Tick()
	}
	deliver(nodes)
	if ns[1].role != Leader {
		t.Fatalf("node 1 never became leader: role=%v", ns[1].role)
	}

	if err := ns[1].ProposeConfChange([]NodeID{2, 3}, nil); err != nil {
		t.Fatal(err)
	}
	deliver(nodes)

	if ns[1].membership.Joint {
		t.Fatal("transition never finalized")
	}
	if ns[1].isVoter(1) {
		t.Fatal("node 1 is still a voter after removing itself from the configuration")
	}
	if ns[1].role == Leader {
		t.Fatal("node 1 is still acting as leader after being removed from the voter set")
	}
}

// TestJointConfigCannotCommitAcrossANewConfigPartition drives the actual replication
// path, rather than only calling HasQuorum: old voters 1 and 2 acknowledge the joint
// entry while every New-only voter is partitioned away. That is a majority of Old but
// not New, and must leave the transition uncommitted.
func TestJointConfigCannotCommitAcrossANewConfigPartition(t *testing.T) {
	ids := []NodeID{1, 2, 3, 4, 5}
	ns := newConfChangeGroup(t, ids, []NodeID{1, 2, 3})
	nodes := nodesAsInterface(ns)
	for i := 0; i < 3; i++ {
		ns[1].Tick()
	}
	deliver(nodes)
	if ns[1].role != Leader {
		t.Fatal("node 1 did not become leader")
	}
	if err := ns[1].ProposeConfChange([]NodeID{1, 2, 3, 4, 5}, nil); err != nil {
		t.Fatal(err)
	}
	// Permit only 1<->2 after the proposal. Node 2 is in Old, but New's required
	// majority cannot be reached without 3, 4, or 5.
	deliverConfChangeFiltered(nodes, func(m Message) bool {
		return (m.From == 1 && m.To == 2) || (m.From == 2 && m.To == 1)
	})
	if ns[1].log.committed != 0 {
		t.Fatalf("joint entry committed through an Old-only partition: committed=%d", ns[1].log.committed)
	}
	if !ns[1].membership.Joint {
		t.Fatal("leader lost its appended joint membership while partitioned")
	}
}

// TestLeaderCrashDuringJointPhaseRecoversSafely covers the failure window Phase 11 is
// designed for. The old leader sends (but cannot commit) C_old,new, then crashes. The
// survivors that received it must elect using both quorums, commit it under that same
// rule, and finish the transition without the failed leader.
func TestLeaderCrashDuringJointPhaseRecoversSafely(t *testing.T) {
	ids := []NodeID{1, 2, 3, 4, 5}
	ns := newConfChangeGroup(t, ids, []NodeID{1, 2, 3})
	all := nodesAsInterface(ns)
	for i := 0; i < 3; i++ {
		ns[1].Tick()
	}
	deliver(all)
	if ns[1].role != Leader {
		t.Fatal("node 1 did not become leader")
	}
	if err := ns[1].ProposeConfChange(ids, nil); err != nil {
		t.Fatal(err)
	}
	// Let nodes 2, 3, and 4 append the joint entry, but lose every acknowledgement so
	// node 1 cannot commit it before it crashes.
	deliverConfChangeFiltered(all, func(m Message) bool {
		return m.Type == MsgAppend && m.From == 1 && (m.To == 2 || m.To == 3 || m.To == 4)
	})
	if ns[1].log.committed != 0 || !ns[2].membership.Joint || !ns[3].membership.Joint || !ns[4].membership.Joint {
		t.Fatal("test setup failed to create an uncommitted joint entry on surviving voters")
	}

	// Node 1 has crashed. Nodes 2, 3, and 4 alone have exactly the two intersecting
	// majorities required by Old={1,2,3} and New={1,2,3,4,5}.
	survivors := map[NodeID]Node{2: ns[2], 3: ns[3], 4: ns[4], 5: ns[5]}
	for i := 0; i < 3; i++ {
		ns[2].Tick()
	}
	deliver(survivors)
	if ns[2].role != Leader {
		t.Fatalf("node 2 did not win a dual-majority election after leader crash: role=%v", ns[2].role)
	}
	if ns[2].membership.Joint || ns[2].log.committed < 2 {
		t.Fatalf("replacement leader did not commit and finalize transition: membership=%#v committed=%d", ns[2].membership, ns[2].log.committed)
	}
}

// TestUncommittedConfChangeIsForgottenWhenOverwritten covers the leader-change conflict
// path: membership is effective on append, but an uncommitted config entry that a later
// leader overwrites must disappear from the follower's quorum state immediately.
func TestUncommittedConfChangeIsForgottenWhenOverwritten(t *testing.T) {
	n := newConfChangeGroup(t, []NodeID{1, 2, 3, 4}, []NodeID{1, 2, 3})[2]
	data, err := marshalConfChange(confChangeEntry{Old: []NodeID{1, 2, 3}, New: []NodeID{1, 2, 3, 4}, Joint: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Step(Message{Type: MsgAppend, From: 1, To: 2, Term: 1, Entries: []Entry{{Index: 1, Term: 1, Data: data}}}); err != nil {
		t.Fatal(err)
	}
	if !n.membership.Joint {
		t.Fatal("joint membership was not applied on append")
	}
	if err := n.Step(Message{Type: MsgAppend, From: 3, To: 2, Term: 2, Entries: []Entry{{Index: 1, Term: 2, Data: []byte("replacement")}}}); err != nil {
		t.Fatal(err)
	}
	if n.membership.Joint || n.isVoter(4) {
		t.Fatalf("overwritten config still affects membership: %#v", n.membership)
	}
}

func TestConfChangeRejectsVoterLearnerOverlap(t *testing.T) {
	ns := newConfChangeGroup(t, []NodeID{1, 2, 3, 4}, []NodeID{1, 2, 3})
	n := ns[1]
	n.role = Leader
	if err := n.ProposeConfChange([]NodeID{1, 2, 3, 4}, []NodeID{4}); err == nil {
		t.Fatal("configuration with voter/learner overlap was accepted")
	}
}
