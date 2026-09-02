package raft

import "testing"

// TestClusterReplicatesCommand exercises election, message delivery, commitment, and
// state-machine application as one deterministic three-replica execution.
func TestClusterReplicatesCommand(t *testing.T) {
	c, err := NewCluster([]NodeID{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	leader, ok := c.Leader()
	if !ok {
		t.Fatal("no leader elected")
	}
	if err := c.Propose(leader, []byte("upsert")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []NodeID{1, 2, 3} {
		got := c.Applied(id)
		if len(got) != 1 || string(got[0]) != "upsert" {
			t.Fatalf("node %d applied %#v", id, got)
		}
	}
}

// TestClusterElectionIsDeterministic proves timeout staggering, not map iteration, picks
// the leader for a fixed replica order.
func TestClusterElectionIsDeterministic(t *testing.T) {
	for run := 0; run < 20; run++ {
		c, err := NewCluster([]NodeID{1, 2, 3})
		if err != nil {
			t.Fatal(err)
		}
		for tick := 0; tick < 3; tick++ {
			if err := c.Tick(); err != nil {
				t.Fatal(err)
			}
		}
		leader, ok := c.Leader()
		if !ok || leader != 1 {
			t.Fatalf("run %d leader=%d elected=%v", run, leader, ok)
		}
	}
}

// TestPartitionedLeaderCannotCommit proves a leader with no reachable majority applies no
// command; accepting it locally would violate Raft's state-machine safety argument.
func TestPartitionedLeaderCannotCommit(t *testing.T) {
	c, err := NewCluster([]NodeID{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	leader, ok := c.Leader()
	if !ok {
		t.Fatal("no leader elected")
	}
	n := c.nodes[leader]
	if err := n.Propose([]byte("unsafe")); err != nil {
		t.Fatal(err)
	}
	if err := c.DeliverFiltered(func(m Message) bool { return m.From != leader && m.To != leader }); err != nil {
		t.Fatal(err)
	}
	if got := c.Applied(leader); len(got) != 0 {
		t.Fatalf("isolated leader applied %#v", got)
	}
}

// TestFilteredMethodsMatchInPackageIsolation proves TickFiltered/ProposeFiltered (the
// exported surface an external fault-injection driver like cmd/torture must use, since it
// cannot reach c.nodes directly) produce the identical isolation behavior as the
// in-package DeliverFiltered pattern TestPartitionedLeaderCannotCommit above uses.
func TestFilteredMethodsMatchInPackageIsolation(t *testing.T) {
	c, err := NewCluster([]NodeID{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.TickFiltered(func(Message) bool { return true }); err != nil {
			t.Fatal(err)
		}
	}
	leader, ok := c.Leader()
	if !ok {
		t.Fatal("no leader elected")
	}
	isolate := func(m Message) bool { return m.From != leader && m.To != leader }
	if err := c.ProposeFiltered(leader, []byte("unsafe"), isolate); err != nil {
		t.Fatal(err)
	}
	if got := c.Applied(leader); len(got) != 0 {
		t.Fatalf("isolated leader applied %#v, want nothing committed while cut off from the majority", got)
	}
	// Reconnecting must let the previously isolated proposal actually commit -- proves
	// isolation was transient (this round only), not a permanent, silently broken cluster.
	if err := c.TickFiltered(func(Message) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if got := c.Applied(leader); len(got) != 1 {
		t.Fatalf("applied = %#v after reconnecting, want the proposal to commit", got)
	}
}

// TestLeaderPrefersHighestTermDuringSustainedIsolation proves Leader() cannot report a
// stale "zombie leader" once the reachable majority has elected a real replacement at a
// higher term. Before this fix, Leader() picked among role==Leader nodes using Go's
// undefined map iteration order, so during a sustained isolation it could return either
// the old, isolated leader (term N) or the new one the majority actually elected
// (term > N) depending on iteration order alone -- corrupting anything built on top of
// it (cmd/torture found this at ~15% of seeds once sustained-window faults were added;
// see docs/notes/06-torture.md).
func TestLeaderPrefersHighestTermDuringSustainedIsolation(t *testing.T) {
	c, err := NewCluster([]NodeID{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	staleLeader, ok := c.Leader()
	if !ok {
		t.Fatal("no leader elected")
	}
	isolate := func(m Message) bool { return m.From != staleLeader && m.To != staleLeader }
	// Isolate the leader long enough for the majority's election timers (staggered
	// 3..7 ticks in NewCluster) to elect a real replacement at a higher term, without
	// ever reconnecting the stale leader.
	for i := 0; i < 10; i++ {
		if err := c.TickFiltered(isolate); err != nil {
			t.Fatal(err)
		}
	}
	newLeader, ok := c.Leader()
	if !ok {
		t.Fatal("no leader reported during sustained isolation")
	}
	if newLeader == staleLeader {
		t.Fatalf("Leader() returned the isolated, stale leader (%d); the majority should have elected a real replacement", staleLeader)
	}
	// This is the property that actually matters: repeated calls must agree, not flip
	// between the stale and real leader depending on map iteration.
	for i := 0; i < 10; i++ {
		if again, _ := c.Leader(); again != newLeader {
			t.Fatalf("Leader() is unstable across repeated calls: got %d then %d", newLeader, again)
		}
	}
}

// TestAsymmetricPartitionDisruptsHealthyLeader documents a real, unfixed gap: pre-vote
// (Ongaro thesis §9.6) stops a RECONNECTING node's inflated term from disrupting a
// healthy cluster, but does nothing for a node that is PERSISTENTLY cut off from the
// leader alone while still fully connected to every other follower. handleVote and
// handlePreVote (election.go) grant a vote based only on log freshness -- neither checks
// whether the responder currently has a healthy, reachable leader (etcd calls this check
// CheckQuorum; this implementation does not have it). So a follower those OTHER
// followers can still reach wins real elections against them and repeatedly displaces
// the actual leader, purely because they have no way to know it's still alive.
//
// This is why the torture harness could not distinguish a correctly-implemented
// pre-vote from a deliberately weakened one (docs/notes/06-torture.md,
// docs/adr/007-prevote-does-not-cover-persistent-asymmetric-partitions.md): every fault
// it can generate is a full bidirectional isolation, which can never produce this
// scenario, and this scenario doesn't distinguish the two implementations anyway --
// proven below by running it against the real, unmodified election path.
func TestAsymmetricPartitionDisruptsHealthyLeader(t *testing.T) {
	c, err := NewCluster([]NodeID{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	leader, ok := c.Leader()
	if !ok {
		t.Fatal("no leader elected")
	}
	var disruptor NodeID
	for _, id := range []NodeID{1, 2, 3, 4, 5} {
		if id != leader {
			disruptor = id
			break
		}
	}

	// Cut only the leader<->disruptor link. Every other pair, including
	// disruptor<->every-other-follower, stays fully connected -- this is what makes it
	// asymmetric rather than the harness's full isolation.
	asymmetric := func(m Message) bool {
		cutLink := (m.From == disruptor && m.To == leader) || (m.From == leader && m.To == disruptor)
		return !cutLink
	}

	sawDisruption := false
	for i := 0; i < 15; i++ {
		if err := c.TickFiltered(asymmetric); err != nil {
			t.Fatal(err)
		}
		if current, ok := c.Leader(); ok && current == disruptor {
			sawDisruption = true
			break
		}
	}
	if !sawDisruption {
		t.Fatal("expected the disruptor to eventually win an election against the leader-reachable followers, proving the asymmetric-partition gap is real")
	}
}

// TestTransferLeadershipToCaughtUpPeer proves the docs/bugs/003 fix's core primitive: a
// leader can hand leadership to a specific, fully-replicated peer via MsgTimeoutNow, and
// that peer wins the resulting election without waiting out a normal randomized timeout.
func TestTransferLeadershipToCaughtUpPeer(t *testing.T) {
	c, err := NewCluster([]NodeID{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	leader, ok := c.Leader()
	if !ok || leader != 1 {
		t.Fatalf("leader=%d elected=%v, want 1", leader, ok)
	}
	// Replicate one entry first so the transfer target has something to be caught up on,
	// not just the empty initial log every replica starts with.
	if err := c.Propose(leader, []byte("v")); err != nil {
		t.Fatal(err)
	}

	target := NodeID(2)
	if err := c.TransferLeadershipTo(leader, target); err != nil {
		t.Fatalf("TransferLeadershipTo: %v", err)
	}
	newLeader, ok := c.Leader()
	if !ok || newLeader != target {
		t.Fatalf("leader after transfer=%d elected=%v, want %d", newLeader, ok, target)
	}
}

// TestTransferLeadershipRejectsUncaughtUpPeer proves a leader refuses to hand leadership
// to a peer whose log is behind -- sending MsgTimeoutNow to that peer would let it win an
// election and then be unable to reconstruct entries the old leader already committed,
// violating Raft's leader-completeness property.
func TestTransferLeadershipRejectsUncaughtUpPeer(t *testing.T) {
	c, err := NewCluster([]NodeID{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	leader, ok := c.Leader()
	if !ok || leader != 1 {
		t.Fatalf("leader=%d elected=%v, want 1", leader, ok)
	}
	// Isolate node 2 before proposing, so its log falls behind nodes 1 and 3.
	behind := NodeID(2)
	isolate := func(m Message) bool { return m.From != behind && m.To != behind }
	if err := c.ProposeFiltered(leader, []byte("v"), isolate); err != nil {
		t.Fatal(err)
	}

	if err := c.TransferLeadershipTo(leader, behind); err == nil {
		t.Fatal("expected TransferLeadershipTo to reject a peer that has not caught up")
	}
	if current, ok := c.Leader(); !ok || current != leader {
		t.Fatalf("leader after rejected transfer=%d elected=%v, want unchanged %d", current, ok, leader)
	}
}

// TestTransferLeadershipRequiresLeader proves a follower cannot initiate a transfer --
// only the current leader has anything to hand off, and Node.TransferLeadershipTo's own
// caught-up check only makes sense relative to a leader's own log.
func TestTransferLeadershipRequiresLeader(t *testing.T) {
	c, err := NewCluster([]NodeID{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	leader, ok := c.Leader()
	if !ok || leader != 1 {
		t.Fatalf("leader=%d elected=%v, want 1", leader, ok)
	}
	var follower NodeID
	for _, id := range []NodeID{1, 2, 3} {
		if id != leader {
			follower = id
			break
		}
	}
	if err := c.TransferLeadershipTo(follower, leader); err == nil {
		t.Fatal("expected TransferLeadershipTo to reject a call from a non-leader")
	}
}
