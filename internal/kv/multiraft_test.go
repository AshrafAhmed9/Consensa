package kv

import (
	"github.com/ashraf/consensa/internal/raft"
	"testing"
)

// TestMultiRaftTicksIndependentGroups proves one scheduler round elects leaders for separate ranges.
func TestMultiRaftTicksIndependentGroups(t *testing.T) {
	m := NewMultiRaft()
	for _, id := range []uint64{1, 2} {
		if e := m.AddGroup(id, []raft.NodeID{1, 2, 3}); e != nil {
			t.Fatal(e)
		}
	}
	for i := 0; i < 3; i++ {
		if e := m.Tick(); e != nil {
			t.Fatal(e)
		}
	}
	for _, id := range []uint64{1, 2} {
		if leader, ok := m.Leader(id); !ok || leader != 1 {
			t.Fatalf("range %d leader=%d elected=%v", id, leader, ok)
		}
	}
}

// TestMultiRaftProposesToTargetRange proves commands do not leak between independently replicated spans.
func TestMultiRaftProposesToTargetRange(t *testing.T) {
	m := NewMultiRaft()
	for _, id := range []uint64{1, 2} {
		if err := m.AddGroup(id, []raft.NodeID{1, 2, 3}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := m.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Propose(2, []byte("range-two")); err != nil {
		t.Fatal(err)
	}
	for _, replica := range []raft.NodeID{1, 2, 3} {
		if got := m.Applied(2, replica); len(got) != 1 || string(got[0]) != "range-two" {
			t.Fatalf("range 2 replica %d = %#v", replica, got)
		}
		if got := m.Applied(1, replica); len(got) != 0 {
			t.Fatalf("range 1 replica %d received %#v", replica, got)
		}
	}
}

// TestMultiRaftAppliesRangeCommandsToEveryReplica proves only committed Raft commands
// change range state, and that independently hosted ranges do not share key/value state.
func TestMultiRaftAppliesRangeCommandsToEveryReplica(t *testing.T) {
	m := NewMultiRaft()
	for _, id := range []uint64{1, 2} {
		if err := m.AddGroup(id, []raft.NodeID{1, 2, 3}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := m.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Put(2, []byte("vector/42"), []byte("payload")); err != nil {
		t.Fatal(err)
	}
	for _, replica := range []raft.NodeID{1, 2, 3} {
		got, err := m.Get(2, replica, []byte("vector/42"))
		if err != nil || string(got) != "payload" {
			t.Fatalf("range 2 replica %d = %q, %v", replica, got, err)
		}
		if _, err := m.Get(1, replica, []byte("vector/42")); err == nil {
			t.Fatalf("range 1 replica %d leaked range 2 state", replica)
		}
	}
	if err := m.Delete(2, []byte("vector/42")); err != nil {
		t.Fatal(err)
	}
	for _, replica := range []raft.NodeID{1, 2, 3} {
		if _, err := m.Get(2, replica, []byte("vector/42")); err == nil {
			t.Fatalf("range 2 replica %d retained deleted key", replica)
		}
	}
}
