package raft

import (
	"testing"

	"github.com/ashraf/consensa/internal/storage"
)

// TestPersisterDurablyRestoresHardState proves a Ready hard state survives a storage restart.
func TestPersisterDurablyRestoresHardState(t *testing.T) {
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPersister(db)
	if err := p.Persist(Ready{HardState: HardState{Term: 4, Vote: 2, Commit: 7}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = storage.Open(storage.Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, err := NewPersister(db).LoadHardState()
	if err != nil || got != (HardState{Term: 4, Vote: 2, Commit: 7}) {
		t.Fatalf("state = %#v, %v", got, err)
	}
}

// TestPersisterDurablyRestoresSnapshot proves snapshot install data survives a storage restart.
func TestPersisterDurablyRestoresSnapshot(t *testing.T) {
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPersister(db)
	want := Snapshot{Index: 8, Term: 3, Data: []byte("graph-state")}
	if err := p.Persist(Ready{Snapshot: want}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = storage.Open(storage.Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, err := NewPersister(db).LoadSnapshot()
	if err != nil || got.Index != want.Index || got.Term != want.Term || string(got.Data) != string(want.Data) {
		t.Fatalf("snapshot=%#v, %v", got, err)
	}
}

// TestRecoverNodeReplaysCommittedEntries proves a crash after durable commit but before
// state-machine apply exposes the same committed command after restart.
func TestRecoverNodeReplaysCommittedEntries(t *testing.T) {
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	persist := NewPersister(db)
	entry := Entry{Index: 1, Term: 2, Data: []byte("set x")}
	if err := persist.Persist(Ready{HardState: HardState{Term: 2, Vote: 1, Commit: 1}, Entries: []Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = storage.Open(storage.Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	node, err := RecoverNode(Config{ID: 1, Peers: []NodeID{1}, ElectionTick: 3, HeartbeatTick: 1}, NewPersister(db))
	if err != nil {
		t.Fatal(err)
	}
	ready := node.Ready()
	if len(ready.CommittedEntries) != 1 || string(ready.CommittedEntries[0].Data) != "set x" {
		t.Fatalf("recovered committed entries = %#v", ready.CommittedEntries)
	}
}

func TestRecoverNodeRestoresMembershipFromSnapshot(t *testing.T) {
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	persist := NewPersister(db)
	snapshot := Snapshot{Index: 8, Term: 3, ConfState: ConfState{Old: []NodeID{2, 3, 4}}}
	if err := persist.Persist(Ready{HardState: HardState{Term: 3, Commit: 8}, Snapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = storage.Open(storage.Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	recovered, err := RecoverNode(Config{ID: 1, Peers: []NodeID{1, 2, 3, 4}, Learners: []NodeID{4}, ElectionTick: 3, HeartbeatTick: 1}, NewPersister(db))
	if err != nil {
		t.Fatal(err)
	}
	n := recovered.(*node)
	if n.isVoter(1) || !n.isVoter(4) || len(n.membership.Old) != 3 {
		t.Fatalf("recovered membership = %#v, want stable {2,3,4}", n.membership)
	}
}

// TestRecoverNodeRestoresUncommittedJointMembership covers a crash while the first
// configuration entry is replicated but not committed. Joint consensus takes effect on
// append, so recovery must preserve that state rather than waiting for the entry to apply.
func TestRecoverNodeRestoresUncommittedJointMembership(t *testing.T) {
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	persist := NewPersister(db)
	data, err := marshalConfChange(confChangeEntry{Old: []NodeID{1, 2, 3}, New: []NodeID{1, 2, 3, 4}, Joint: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := persist.Persist(Ready{HardState: HardState{Term: 2}, Entries: []Entry{{Index: 1, Term: 2, Data: data}}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = storage.Open(storage.Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	recovered, err := RecoverNode(Config{ID: 1, Peers: []NodeID{1, 2, 3, 4}, Learners: []NodeID{4}, ElectionTick: 3, HeartbeatTick: 1}, NewPersister(db))
	if err != nil {
		t.Fatal(err)
	}
	n := recovered.(*node)
	if !n.membership.Joint || !n.isVoter(4) || n.log.committed != 0 {
		t.Fatalf("recovered state = membership=%#v commit=%d, want uncommitted joint config", n.membership, n.log.committed)
	}
}
