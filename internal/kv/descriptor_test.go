package kv

import (
	"fmt"
	"github.com/ashraf/consensa/internal/raft"
	"testing"
)

// TestRouterRefreshesAfterDescriptorChange proves a stale client can discard cache and reroute.
func TestRouterRefreshesAfterDescriptorChange(t *testing.T) {
	m, e := NewMeta([]Descriptor{{ID: 1, Start: nil, End: []byte("m"), Replicas: []raft.NodeID{1}}, {ID: 2, Start: []byte("m"), End: nil, Replicas: []raft.NodeID{2}}})
	if e != nil {
		t.Fatal(e)
	}
	r := NewRouter(m)
	d, e := r.Route([]byte("z"))
	if e != nil || d.ID != 2 {
		t.Fatalf("route=%v,%v", d, e)
	}
	r.Refresh()
	d, e = r.Route([]byte("a"))
	if e != nil || d.ID != 1 {
		t.Fatalf("route=%v,%v", d, e)
	}
}

// TestMetaReplacePublishesSplitAtomically proves routing resolves through a complete child catalog.
func TestMetaReplacePublishesSplitAtomically(t *testing.T) {
	m, err := NewMeta([]Descriptor{{ID: 1, Start: nil, End: nil, Replicas: []raft.NodeID{1, 2, 3}}})
	if err != nil {
		t.Fatal(err)
	}
	parent, _ := m.Lookup([]byte("m"))
	left, right, err := SplitDescriptor(parent, []byte("m"), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Replace([]Descriptor{left, right}); err != nil {
		t.Fatal(err)
	}
	if got, err := m.Lookup([]byte("a")); err != nil || got.ID != 2 {
		t.Fatalf("left=%v,%v", got, err)
	}
	if got, err := m.Lookup([]byte("z")); err != nil || got.ID != 3 {
		t.Fatalf("right=%v,%v", got, err)
	}
}

// TestRoutedKVSeparatesStaticRanges proves key-addressed requests reach their descriptor's
// independent Raft group, rather than relying on the caller to choose a range ID.
func TestRoutedKVSeparatesStaticRanges(t *testing.T) {
	meta, err := NewMeta([]Descriptor{
		{ID: 1, Start: nil, End: []byte("m"), Replicas: []raft.NodeID{1, 2, 3}},
		{ID: 2, Start: []byte("m"), End: nil, Replicas: []raft.NodeID{1, 2, 3}},
	})
	if err != nil {
		t.Fatal(err)
	}
	multiRaft := NewMultiRaft()
	for _, rangeID := range []uint64{1, 2} {
		if err := multiRaft.AddGroup(rangeID, []raft.NodeID{1, 2, 3}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := multiRaft.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	client := NewRoutedKV(NewRouter(meta), multiRaft)
	if err := client.Put([]byte("apple"), []byte("left")); err != nil {
		t.Fatal(err)
	}
	if err := client.Put([]byte("zebra"), []byte("right")); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"apple": "left", "zebra": "right"} {
		got, err := client.Get([]byte(key))
		if err != nil || string(got) != want {
			t.Fatalf("Get(%q) = %q, %v; want %q", key, got, err, want)
		}
	}
	if got := multiRaft.Applied(1, 1); len(got) != 1 {
		t.Fatalf("left range applied %d commands, want 1", len(got))
	}
	if got := multiRaft.Applied(2, 1); len(got) != 1 {
		t.Fatalf("right range applied %d commands, want 1", len(got))
	}
}

// TestRoutedKVRefreshesAfterStaticRangeMove proves a client with a cached parent range
// retries through replacement metadata after the original group is removed.
func TestRoutedKVRefreshesAfterStaticRangeMove(t *testing.T) {
	replicas := []raft.NodeID{1, 2, 3}
	meta, err := NewMeta([]Descriptor{{ID: 1, Replicas: replicas}})
	if err != nil {
		t.Fatal(err)
	}
	multiRaft := NewMultiRaft()
	if err := multiRaft.AddGroup(1, replicas); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := multiRaft.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	client := NewRoutedKV(NewRouter(meta), multiRaft)
	// Seed the client cache while the parent still owns the full keyspace.
	if _, err := client.Get([]byte("zebra")); err == nil {
		t.Fatal("expected absent key")
	}
	parent, _ := meta.Lookup([]byte("zebra"))
	left, right, err := SplitDescriptor(parent, []byte("m"), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, descriptor := range []Descriptor{left, right} {
		if err := multiRaft.AddGroup(descriptor.ID, replicas); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := multiRaft.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if err := meta.Replace([]Descriptor{left, right}); err != nil {
		t.Fatal(err)
	}
	if err := multiRaft.RemoveGroup(parent.ID); err != nil {
		t.Fatal(err)
	}
	if err := client.Put([]byte("zebra"), []byte("moved")); err != nil {
		t.Fatal(err)
	}
	got, err := client.Get([]byte("zebra"))
	if err != nil || string(got) != "moved" {
		t.Fatalf("Get after refresh = %q, %v", got, err)
	}
	if got := multiRaft.Applied(right.ID, 1); len(got) != 1 {
		t.Fatalf("replacement range applied %d commands, want 1", len(got))
	}
}

// TestRoutedKVEightStaticRanges proves a three-replica cluster can host the plan's
// minimum eight key spans without cross-range command delivery.
func TestRoutedKVEightStaticRanges(t *testing.T) {
	replicas := []raft.NodeID{1, 2, 3}
	descriptors := make([]Descriptor, 8)
	for i := range descriptors {
		descriptors[i] = Descriptor{ID: uint64(i + 1), Replicas: replicas}
		if i > 0 {
			descriptors[i].Start = []byte{byte('a' + i)}
		}
		if i < len(descriptors)-1 {
			descriptors[i].End = []byte{byte('a' + i + 1)}
		}
	}
	meta, err := NewMeta(descriptors)
	if err != nil {
		t.Fatal(err)
	}
	multiRaft := NewMultiRaft()
	for _, descriptor := range descriptors {
		if err := multiRaft.AddGroup(descriptor.ID, replicas); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := multiRaft.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	client := NewRoutedKV(NewRouter(meta), multiRaft)
	for i, descriptor := range descriptors {
		key := []byte{byte('a' + i)}
		value := []byte(fmt.Sprintf("range-%d", descriptor.ID))
		if err := client.Put(key, value); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
		got, err := client.Get(key)
		if err != nil || string(got) != string(value) {
			t.Fatalf("Get(%q) = %q, %v; want %q", key, got, err, value)
		}
		if applied := multiRaft.Applied(descriptor.ID, 1); len(applied) != 1 {
			t.Fatalf("range %d applied %d commands, want 1", descriptor.ID, len(applied))
		}
	}
}
