package raft

import "testing"

// TestDriverPersistsBeforeSend protects Raft's critical durable-before-visible ordering.
func TestDriverPersistsBeforeSend(t *testing.T) {
	n := newTestNode(t, 1)
	order := []string{}
	d := Driver{Node: n, Persist: func(Ready) error { order = append(order, "persist"); return nil }, Send: func(Message) error { order = append(order, "send"); return nil }, Apply: func(Entry) error { return nil }}
	n.Tick()
	n.Tick()
	n.Tick()
	if err := d.Drive(); err != nil {
		t.Fatal(err)
	}
	if len(order) < 2 || order[0] != "persist" || order[1] != "send" {
		t.Fatalf("ordering = %v", order)
	}
}
