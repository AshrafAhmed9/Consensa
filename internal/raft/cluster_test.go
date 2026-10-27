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

// TestFilteredMethodsMatchInPackageIsolation checks that TickFiltered and ProposeFiltered
// (what cmd/torture uses from outside the package) isolate a node exactly the way the
// in-package DeliverFiltered call in TestPartitionedLeaderCannotCommit does.
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

// TestLeaderPrefersHighestTermDuringSustainedIsolation proves Leader() does not report an
// old isolated leader once the rest of the cluster has elected a new one at a higher
// term. Leader() used to pick among the nodes calling themselves leader in random map
// order, so it could flip between the old and new leader.
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

// TestAsymmetricPartitionDisruptsHealthyLeader documents a known gap. Pre-vote stops a
// node that was cut off and then reconnects from disrupting the cluster. It does not help
// when one follower is permanently cut off from the leader only, while still reaching
// every other follower. Votes are granted based only on how up to date the candidate's
// log is, and nothing checks whether the voter still has a working leader (etcd calls
// that check CheckQuorum, which is not implemented here). So that follower keeps winning
// elections and displacing the healthy leader. The torture harness cannot produce this
// case, because its faults always cut a node off from everyone.
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
