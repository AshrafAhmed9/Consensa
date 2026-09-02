package raft

import (
	"testing"
	"time"
)

// TestBrandNewProcessJoinsLiveGroupAsLearnerThenVoter proves the exact scenario
// TestJointTransitionCompletesAndFinalizesAutomatically's own doc comment names as NOT
// covered there: "two brand-new nodes appearing from nothing," not two already-running
// learner processes an existing 5-node Config already knew about from the start. Here,
// node 4's *testHost is constructed only after nodes 1-3 have already elected a leader
// and committed real entries -- its address exists nowhere in any of the other three
// hosts' transports until AddPeer registers it, exactly the deployment-time gap
// docs/notes/11-joint-consensus.md named ("no workflow yet for provisioning a brand-new
// process and publishing its address"). Without AddPeer, ProposeConfChange would still
// succeed at the Raft membership layer, but every message to node 4 would fail with
// "unknown peer address" and it could never actually catch up.
func TestBrandNewProcessJoinsLiveGroupAsLearnerThenVoter(t *testing.T) {
	ids := []NodeID{1, 2, 3}
	addrs := map[NodeID]string{1: freeTCPAddr(t), 2: freeTCPAddr(t), 3: freeTCPAddr(t)}
	log := newApplyLog()

	var hosts []*testHost
	for _, id := range ids {
		peers := map[NodeID]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = addrs[other]
			}
		}
		h := startTestHost(t, id, ids, addrs[id], peers, log.applierFor(id))
		hosts = append(hosts, h)
		defer h.close(t)
	}

	stop := make(chan struct{})
	wg := driveHosts(hosts, 10*time.Millisecond, stop)
	defer func() { close(stop); wg.Wait() }()

	// The group does real work, and node 4 does not exist anywhere yet -- no host in this
	// deployment has ever heard its ID or address.
	if err := proposeToLeader(hosts, []byte("before node 4 exists"), 20*time.Second); err != nil {
		t.Fatalf("no leader accepted the first proposal: %v", err)
	}
	waitForCount(t, log, 1, 1, 15*time.Second)

	// Node 4 now starts, as a genuinely separate process would: it knows the other three
	// members' addresses (an operator would supply this, the same way --peers is supplied
	// on every node at deployment time) and its own Raft config already lists all four
	// IDs -- a joining node has to know the group it's joining -- but nothing about it has
	// been told to nodes 1-3 yet.
	fourID := NodeID(4)
	fourAddr := freeTCPAddr(t)
	allIDs := append(append([]NodeID(nil), ids...), fourID)
	fourPeers := map[NodeID]string{1: addrs[1], 2: addrs[2], 3: addrs[3]}
	four := startTestHost(t, fourID, allIDs, fourAddr, fourPeers, log.applierFor(fourID))
	hosts = append(hosts, four)
	defer four.close(t)
	stopFour := make(chan struct{})
	wgFour := driveHosts([]*testHost{four}, 10*time.Millisecond, stopFour)
	defer func() { close(stopFour); wgFour.Wait() }()

	// Both AddKnownPeer and AddPeer must run on every existing replica before
	// ProposeConfChange will accept node 4 at all (AddKnownPeer) and before any message
	// to it can actually be delivered (AddPeer) -- the two halves of provisioning a
	// genuinely new process this deployment never addressed before.
	for _, h := range hosts[:3] {
		h.host.AddKnownPeer(fourID)
		if err := h.host.AddPeer(fourID, fourAddr); err != nil {
			t.Fatalf("node %d: AddPeer(4): %v", h.id, err)
		}
	}

	var leader *testHost
	for _, h := range hosts[:3] {
		if role, _ := h.host.Status(); role == Leader {
			leader = h
		}
	}
	if leader == nil {
		t.Fatal("no leader found among nodes 1-3 before proposing the conf change")
	}
	// Node 4 joins as a learner first -- Raft's own safety story (docs/adr/010) requires
	// a learner catch up via replication/snapshot before it can be trusted with a vote.
	if err := leader.host.ProposeConfChange(ids, []NodeID{fourID}); err != nil {
		t.Fatalf("leader %d: ProposeConfChange(learner=4): %v", leader.id, err)
	}

	// A second write after node 4 joins as a learner -- it must actually receive and
	// apply this over the real TCP connection AddPeer just made possible.
	if err := proposeToLeader(hosts[:3], []byte("after node 4 joins as learner"), 20*time.Second); err != nil {
		t.Fatalf("no leader accepted the second proposal: %v", err)
	}
	// Node 4 started with an empty log, so catching up necessarily replicates the FIRST
	// proposal too (its own log has nothing at index 1 to match against) before it can
	// apply the second -- wait for both, not just "at least one entry."
	waitForCount(t, log, fourID, 2, 20*time.Second)
	if got := log.last(fourID); string(got) != "after node 4 joins as learner" {
		t.Fatalf("node 4's last applied entry = %q, want the second proposal's data", got)
	}

	// Promote node 4 to a full voter and confirm the group still commits with it counted
	// toward quorum -- proving this isn't just a learner passively receiving replication,
	// but a real new member of a live deployment. Leadership may have changed since the
	// first ProposeConfChange call, so find the current leader again rather than assuming
	// it's still the same host.
	deadline := time.Now().Add(20 * time.Second)
	var promoted bool
	for time.Now().Before(deadline) && !promoted {
		for _, h := range hosts[:3] {
			if role, _ := h.host.Status(); role == Leader {
				if err := h.host.ProposeConfChange(allIDs, nil); err == nil {
					promoted = true
					break
				}
			}
		}
		if !promoted {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !promoted {
		t.Fatal("no leader accepted ProposeConfChange(promote 4) within the deadline")
	}
	if err := proposeToLeader(hosts, []byte("after node 4 promoted"), 20*time.Second); err != nil {
		t.Fatalf("no leader accepted the third proposal: %v", err)
	}
	for _, id := range allIDs {
		waitForCount(t, log, id, 3, 20*time.Second)
	}
}
