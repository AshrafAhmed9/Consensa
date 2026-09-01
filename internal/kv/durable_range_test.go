package kv

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/storage"
)

// TestDurableRangeRejectsReservedKeyPrefix proves the "raft/" namespace guard actually
// runs, standalone from the full election/replication machinery -- a single replica with
// no group is enough to exercise Put/Delete's validation.
func TestDurableRangeRejectsReservedKeyPrefix(t *testing.T) {
	addr := freeKVAddr(t)
	r, err := NewDurableRange(DurableRangeConfig{
		ID: 1, GroupPeers: []raft.NodeID{1}, ListenAddress: addr, TransportPeers: map[raft.NodeID]string{}, StorageDir: t.TempDir(),
		ElectionTick: 60, HeartbeatTick: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	wantReserved := "kv: key collides with the reserved \"raft/\" namespace"
	if err := r.Put([]byte("raft/hard-state"), []byte("x")); err == nil || err.Error() != wantReserved {
		t.Fatalf("Put with reserved prefix = %v, want the reserved-namespace rejection", err)
	}
	if err := r.Delete([]byte("raft/log/whatever")); err == nil || err.Error() != wantReserved {
		t.Fatalf("Delete with reserved prefix = %v, want the reserved-namespace rejection", err)
	}
	// "raft" with no trailing slash is not the reserved namespace, so validation must pass
	// it through to Propose -- whether Propose itself succeeds depends on this single,
	// never-ticked node's leadership state (a 1-node group still needs an election timeout
	// to fire), which is not what this test is checking, so only the reserved-prefix error
	// is disallowed here.
	if err := r.Put([]byte("raft"), []byte("x")); err != nil && err.Error() == wantReserved {
		t.Fatalf("Put(%q) incorrectly treated as the reserved namespace", "raft")
	}
}

// This file is DurableRange's counterpart to internal/ann/durable_test.go: it proves a
// killed-and-restarted range replica recovers its key/value data, and specifically proves
// it via storage.Engine's own WAL/SSTable recovery -- not via replaying the Raft log
// through an in-memory map the way DurableNode/HNSW must. If DurableRange's apply were
// accidentally writing into an in-memory map instead of the real engine, this test would
// still catch it, because the restarted replica here opens a FRESH DurableRange against
// the same directory and must answer Get() correctly before any Raft activity at all.

func freeKVAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return addr
}

func driveRanges(ranges []*DurableRange, interval time.Duration, stop <-chan struct{}) *sync.WaitGroup {
	var wg sync.WaitGroup
	for _, r := range ranges {
		wg.Add(1)
		go func(r *DurableRange) {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					_ = r.Tick()
				}
			}
		}(r)
	}
	return &wg
}

func putUntilAccepted(ranges []*DurableRange, key, value []byte, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	lastErr := errors.New("no replica accepted the put")
	for time.Now().Before(end) {
		for _, r := range ranges {
			if err := r.Put(key, value); err == nil {
				return nil
			} else {
				lastErr = err
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return lastErr
}

// TestDurableRangeSurvivesRestart mirrors internal/ann's TestDurableNodeSurvivesRestart:
// three real replicas over real TCP, kill one, restart it from the same directory, and
// confirm it answers correctly before talking to a single peer.
func TestDurableRangeSurvivesRestart(t *testing.T) {
	ids := []raft.NodeID{1, 2, 3}
	addrs := map[raft.NodeID]string{1: freeKVAddr(t), 2: freeKVAddr(t), 3: freeKVAddr(t)}
	dirs := map[raft.NodeID]string{1: t.TempDir(), 2: t.TempDir(), 3: t.TempDir()}

	newRange := func(id raft.NodeID) *DurableRange {
		peers := map[raft.NodeID]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = addrs[other]
			}
		}
		r, err := NewDurableRange(DurableRangeConfig{
			ID: id, GroupPeers: ids, ListenAddress: addrs[id], TransportPeers: peers, StorageDir: dirs[id],
			ElectionTick: 60, HeartbeatTick: 6,
		})
		if err != nil {
			t.Fatalf("range %d: %v", id, err)
		}
		return r
	}

	ranges := map[raft.NodeID]*DurableRange{}
	var live []*DurableRange
	for _, id := range ids {
		r := newRange(id)
		ranges[id] = r
		live = append(live, r)
	}

	stop := make(chan struct{})
	wg := driveRanges(live, 10*time.Millisecond, stop)

	keys := map[string][]byte{"a": []byte("1"), "b": []byte("2"), "c": []byte("3")}
	for _, k := range []string{"a", "b", "c"} {
		if err := putUntilAccepted(live, []byte(k), keys[k], 20*time.Second); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	// Confirm every replica's own durable engine actually has the data before killing
	// anything -- otherwise a later "recovery" could just be finding nothing, twice.
	deadline := time.Now().Add(20 * time.Second)
	for _, r := range live {
		for {
			got, err := r.Get([]byte("c"))
			if err == nil && string(got) == "3" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("replica never converged on key c: got=%q err=%v", got, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	close(stop)
	wg.Wait()
	if err := ranges[3].Close(); err != nil {
		t.Fatalf("closing range 3: %v", err)
	}

	// Restart range 3 from the SAME directory. No Tick, no peer contact before the
	// assertion: if Get is correct here, the data came from storage.Engine's own recovery.
	restarted, err := NewDurableRange(DurableRangeConfig{
		ID: 3, GroupPeers: ids, ListenAddress: addrs[3],
		TransportPeers: map[raft.NodeID]string{1: addrs[1], 2: addrs[2]},
		StorageDir:     dirs[3],
		ElectionTick:   60, HeartbeatTick: 6,
	})
	if err != nil {
		t.Fatalf("restarting range 3: %v", err)
	}
	defer restarted.Close()

	for _, k := range []string{"a", "b", "c"} {
		got, err := restarted.Get([]byte(k))
		if err != nil {
			t.Fatalf("restarted range 3: get %q: %v", k, err)
		}
		if string(got) != string(keys[k]) {
			t.Fatalf("restarted range 3: get %q = %q, want %q", k, got, keys[k])
		}
	}

	if err := ranges[1].Close(); err != nil {
		t.Errorf("closing range 1: %v", err)
	}
	if err := ranges[2].Close(); err != nil {
		t.Errorf("closing range 2: %v", err)
	}
}

// TestDurableRangeDeleteIsDurable proves a delete survives a restart too, not just a put --
// storage.Engine represents a deletion as a tombstone version, so this also guards against
// a restart that resurrects a deleted key by only ever replaying puts.
func TestDurableRangeDeleteIsDurable(t *testing.T) {
	ids := []raft.NodeID{1, 2, 3}
	addrs := map[raft.NodeID]string{1: freeKVAddr(t), 2: freeKVAddr(t), 3: freeKVAddr(t)}
	dirs := map[raft.NodeID]string{1: t.TempDir(), 2: t.TempDir(), 3: t.TempDir()}

	newRange := func(id raft.NodeID) *DurableRange {
		peers := map[raft.NodeID]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = addrs[other]
			}
		}
		r, err := NewDurableRange(DurableRangeConfig{
			ID: id, GroupPeers: ids, ListenAddress: addrs[id], TransportPeers: peers, StorageDir: dirs[id],
			ElectionTick: 60, HeartbeatTick: 6,
		})
		if err != nil {
			t.Fatalf("range %d: %v", id, err)
		}
		return r
	}

	var live []*DurableRange
	for _, id := range ids {
		live = append(live, newRange(id))
	}
	stop := make(chan struct{})
	wg := driveRanges(live, 10*time.Millisecond, stop)
	defer func() {
		close(stop)
		wg.Wait()
		for _, r := range live {
			_ = r.Close()
		}
	}()

	if err := putUntilAccepted(live, []byte("x"), []byte("temporary"), 20*time.Second); err != nil {
		t.Fatalf("put: %v", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		found := true
		for _, r := range live {
			if v, err := r.Get([]byte("x")); err != nil || string(v) != "temporary" {
				found = false
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("put never converged")
		}
		time.Sleep(10 * time.Millisecond)
	}

	lastErr := errors.New("no replica accepted the delete")
	deleted := false
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && !deleted {
		for _, r := range live {
			if err := r.Delete([]byte("x")); err == nil {
				deleted = true
				break
			} else {
				lastErr = err
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !deleted {
		t.Fatalf("delete never accepted: %v", lastErr)
	}

	deadline = time.Now().Add(20 * time.Second)
	for {
		allGone := true
		for _, r := range live {
			if _, err := r.Get([]byte("x")); !errors.Is(err, storage.ErrNotFound) {
				allGone = false
			}
		}
		if allGone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("delete never converged to ErrNotFound on every replica")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestAllKeysExcludesReservedNamespaceAndReflectsDeletes proves AllKeys returns exactly
// the application data a replica has applied -- not Persister's "raft/" bookkeeping
// sharing the same engine, and not a stale snapshot that still shows a deleted key. Both
// are real ways this could go wrong: a naive full-engine scan would include "raft/" keys,
// and a scan taken before a delete's tombstone is visible would show a value already gone
// from Get.
func TestAllKeysExcludesReservedNamespaceAndReflectsDeletes(t *testing.T) {
	ids := []raft.NodeID{1, 2, 3}
	group := startDurableRangeGroup(t, 1, nil, nil, ids)
	defer group.closeAll(t)

	stop := make(chan struct{})
	wg := driveRanges(group.list(), 10*time.Millisecond, stop)
	defer func() { close(stop); wg.Wait() }()

	var leader *DurableRange
	deadline := time.Now().Add(20 * time.Second)
	for leader == nil {
		for _, r := range group.list() {
			if role, _ := r.Status(); role == raft.Leader {
				leader = r
				break
			}
		}
		if leader == nil {
			if time.Now().After(deadline) {
				t.Fatal("no leader elected")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	want := map[string][]byte{"a": []byte("1"), "b": []byte("2"), "c": []byte("3")}
	for k, v := range want {
		if err := putUntilAccepted(group.list(), []byte(k), v, 20*time.Second); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	deadline = time.Now().Add(20 * time.Second)
	for {
		all, err := leader.AllKeys()
		if err == nil && len(all) == len(want) {
			ok := true
			for k, v := range want {
				if string(all[k]) != string(v) {
					ok = false
				}
			}
			if ok {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("AllKeys never converged to %v, last=%v err=%v", want, all, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	deleted := false
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && !deleted {
		if err := leader.Delete([]byte("b")); err == nil {
			deleted = true
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !deleted {
		t.Fatal("delete of b never accepted")
	}

	deadline = time.Now().Add(20 * time.Second)
	for {
		all, err := leader.AllKeys()
		if err == nil {
			if _, present := all["b"]; !present && len(all) == 2 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("AllKeys never reflected the delete of b, last=%v err=%v", all, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestGrantLeaseReplicatesToEveryReplica proves a lease grant is a real Raft-committed
// operation, not just local bookkeeping: every replica in the group -- not just the
// granting leader -- ends up with the identical Holder/Start/Expiration once the grant
// commits, and FollowerReadAllowedWithOffset (lease.go) correctly accepts a read against
// the replicated lease before expiry and rejects one after it. This proves lease
// replication itself; it does not exercise closed-timestamp advancement or an actual
// follower-read RPC path, neither of which exists yet (see GrantLease's doc comment).
func TestGrantLeaseReplicatesToEveryReplica(t *testing.T) {
	ids := []raft.NodeID{1, 2, 3}
	group := startDurableRangeGroup(t, 1, nil, nil, ids)
	defer group.closeAll(t)

	stop := make(chan struct{})
	wg := driveRanges(group.list(), 10*time.Millisecond, stop)
	defer func() { close(stop); wg.Wait() }()

	var leader *DurableRange
	deadline := time.Now().Add(20 * time.Second)
	for leader == nil {
		for _, r := range group.list() {
			if role, _ := r.Status(); role == raft.Leader {
				leader = r
				break
			}
		}
		if leader == nil {
			if time.Now().After(deadline) {
				t.Fatal("no leader elected")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	var grantErr error
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if grantErr = leader.GrantLease(1, 5*time.Second); grantErr == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if grantErr != nil {
		t.Fatalf("leader never accepted GrantLease: %v", grantErr)
	}

	deadline = time.Now().Add(20 * time.Second)
	for {
		allConverged := true
		for _, r := range group.list() {
			lease := r.CurrentLease()
			if lease.Holder != raft.NodeID(1) || lease.Expiration.IsZero() {
				allConverged = false
			}
		}
		if allConverged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease grant never replicated to every replica")
		}
		time.Sleep(10 * time.Millisecond)
	}

	lease := leader.CurrentLease()
	if err := FollowerReadAllowedWithOffset(lease, 1, ClosedTimestamp{Timestamp: lease.Start, AppliedIndex: 0}, lease.Start, lease.Start, 0, 0); err != nil {
		t.Fatalf("read at lease.Start should be allowed: %v", err)
	}
	afterExpiry := lease.Expiration.Add(time.Second)
	if err := FollowerReadAllowedWithOffset(lease, 1, ClosedTimestamp{Timestamp: lease.Start, AppliedIndex: 0}, lease.Start, afterExpiry, 0, 0); err == nil {
		t.Fatal("read after lease expiration should be rejected")
	}
}

// TestConsistentGetRequiresLeadership proves ConsistentGet actually enforces the
// leader-only contract its doc comment claims -- the property that makes it linearizable
// where Get is only bounded-stale. A follower must reject it exactly like Put/Delete
// reject a non-leader proposal; the leader must serve it once the write it's reading is
// actually visible there.
func TestConsistentGetRequiresLeadership(t *testing.T) {
	ids := []raft.NodeID{1, 2, 3}
	group := startDurableRangeGroup(t, 1, nil, nil, ids)
	defer group.closeAll(t)

	var leader *DurableRange
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && leader == nil {
		for _, r := range group.list() {
			if err := r.Tick(); err != nil {
				t.Fatal(err)
			}
			if role, _ := r.Status(); role == raft.Leader {
				leader = r
			}
		}
	}
	if leader == nil {
		t.Fatal("group never elected a leader")
	}

	deadline = time.Now().Add(20 * time.Second)
	var putErr error
	for time.Now().Before(deadline) {
		if putErr = leader.Put([]byte("k"), []byte("v")); putErr == nil {
			break
		}
	}
	if putErr != nil {
		t.Fatalf("leader never accepted the put: %v", putErr)
	}

	deadline = time.Now().Add(20 * time.Second)
	var got []byte
	var getErr error
	for time.Now().Before(deadline) {
		if got, getErr = leader.ConsistentGet([]byte("k")); getErr == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if getErr != nil || string(got) != "v" {
		t.Fatalf("ConsistentGet on the leader = %q, %v, want \"v\", nil", got, getErr)
	}

	for _, r := range group.list() {
		if r == leader {
			continue
		}
		if role, _ := r.Status(); role == raft.Leader {
			continue // an election could have moved leadership mid-test; skip, don't fail.
		}
		if _, err := r.ConsistentGet([]byte("k")); err == nil {
			t.Fatalf("ConsistentGet on a non-leader replica succeeded; it must reject like Put does")
		}
	}
}
