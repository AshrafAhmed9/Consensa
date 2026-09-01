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
