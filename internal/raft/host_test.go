package raft

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/storage"
)

// This file is the test TCPTransport and Host never had: internal/raft/cluster_test.go
// proves the pure protocol (Step/Tick/Ready) is correct, but nothing exercised the actual
// production path -- real sockets, a real on-disk WAL via Persister, and a process-level
// leader failure -- so a bug in the transport framing or the Host/Persister wiring could
// pass every other test in the repo undetected. These tests close that gap.

// applyLog records committed entries per node, guarded by its own mutex because Host.Apply
// is invoked from whichever goroutine is driving that host's Tick/Step at the time.
type applyLog struct {
	mu      sync.Mutex
	entries map[NodeID][][]byte
}

type discardTransport struct{}

func (discardTransport) Addr() net.Addr     { return &net.TCPAddr{} }
func (discardTransport) Send(Message) error { return nil }
func (discardTransport) Close() error       { return nil }

func newApplyLog() *applyLog { return &applyLog{entries: map[NodeID][][]byte{}} }

func (a *applyLog) applierFor(id NodeID) func(Entry) error {
	return func(e Entry) error {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.entries[id] = append(a.entries[id], append([]byte(nil), e.Data...))
		return nil
	}
}

func (a *applyLog) count(id NodeID) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.entries[id])
}

func (a *applyLog) last(id NodeID) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	entries := a.entries[id]
	if len(entries) == 0 {
		return nil
	}
	return entries[len(entries)-1]
}

func TestHostDoesNotApplyInternalConfChangeEntries(t *testing.T) {
	db, err := storage.Open(storage.Options{Dir: t.TempDir(), SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	n, err := NewNode(Config{ID: 1, Peers: []NodeID{1}, ElectionTick: 3, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	state := n.(*node)
	state.role, state.term = Leader, 1
	if err := state.ProposeConfChange([]NodeID{1}, nil); err != nil {
		t.Fatal(err)
	}
	state.log.committed = state.log.lastIndex()
	applied := 0
	h := &Host{node: state, persister: NewPersister(db), transport: discardTransport{}, progress: make(chan struct{}), apply: func(Entry) error {
		applied++
		return nil
	}}
	if err := h.driveLocked(); err != nil {
		t.Fatal(err)
	}
	if applied != 0 {
		t.Fatalf("application state machine received %d internal config entries", applied)
	}
}

// freeTCPAddr reserves an OS-assigned loopback port and immediately releases it. There is
// a theoretical reuse race between the Close and the caller's own Listen, but it is the
// standard Go testing idiom for this and is not worth a two-phase listener handoff here.
func freeTCPAddr(t *testing.T) string {
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

// testHost bundles one node's storage, persister, and transport so the test can crash and
// recover it independently of its peers -- which is the entire point of the exercise.
type testHost struct {
	id   NodeID
	dir  string
	db   *storage.DB
	host *Host
}

func startTestHost(t *testing.T, id NodeID, groupPeers []NodeID, addr string, transportPeers map[NodeID]string, apply func(Entry) error) *testHost {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatalf("node %d: open storage: %v", id, err)
	}
	host, err := NewHost(HostConfig{
		Raft:          Config{ID: id, Peers: groupPeers, ElectionTick: 50, HeartbeatTick: 5},
		ListenAddress: addr,
		Peers:         transportPeers,
		Persister:     NewPersister(db),
		Apply:         apply,
	})
	if err != nil {
		t.Fatalf("node %d: start host: %v", id, err)
	}
	return &testHost{id: id, dir: dir, db: db, host: host}
}

func (h *testHost) close(t *testing.T) {
	t.Helper()
	if err := h.host.Close(); err != nil {
		t.Errorf("node %d: close transport: %v", h.id, err)
	}
	if err := h.db.Close(); err != nil {
		t.Errorf("node %d: close storage: %v", h.id, err)
	}
}

// TestHostStaggersEqualElectionTimeouts proves production Hosts never inherit the
// simulator-only failure mode where otherwise identical replicas time out together and
// split every election forever. The offset is deterministic, so a restarted replica
// keeps the same timeout instead of introducing timing-dependent recovery behavior.
func TestHostStaggersEqualElectionTimeouts(t *testing.T) {
	ids := []NodeID{1, 2, 3}
	addrs := map[NodeID]string{1: freeTCPAddr(t), 2: freeTCPAddr(t), 3: freeTCPAddr(t)}
	log := newApplyLog()
	seen := map[int]bool{}
	for _, id := range ids {
		peers := map[NodeID]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = addrs[other]
			}
		}
		h := startTestHost(t, id, ids, addrs[id], peers, log.applierFor(id))
		defer h.close(t)
		timeout := h.host.node.(*node).electionTick
		if seen[timeout] {
			t.Fatalf("node %d reused election timeout %d", id, timeout)
		}
		seen[timeout] = true
	}
}

// driveHosts ticks every host in hosts on its own goroutine every tickInterval until stop
// is closed. Ticking concurrently, rather than round-robin from the test goroutine, is
// what actually exercises the mutex inside Host and the concurrent inbound TCP handlers.
func driveHosts(hosts []*testHost, tickInterval time.Duration, stop <-chan struct{}) *sync.WaitGroup {
	var wg sync.WaitGroup
	for _, h := range hosts {
		wg.Add(1)
		go func(h *testHost) {
			defer wg.Done()
			ticker := time.NewTicker(tickInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					// A tick error just means "not leader yet" or a transient dial failure
					// against a peer that hasn't started listening yet; both are expected
					// during startup and after a deliberate node failure in these tests.
					_ = h.host.Tick()
				}
			}
		}(h)
	}
	return &wg
}

// proposeToLeader retries Propose against every live host until one accepts it (i.e. is
// leader) or deadline elapses. Host does not expose Role, which is deliberate -- Propose's
// own "not leader" error is the real API a client has to handle, so the test uses exactly
// that instead of reaching into node internals.
func proposeToLeader(hosts []*testHost, data []byte, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	lastErr := errors.New("no host accepted the proposal")
	for time.Now().Before(end) {
		for _, h := range hosts {
			if err := h.host.Propose(data); err == nil {
				return nil
			} else {
				lastErr = err
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return lastErr
}

func waitForCount(t *testing.T, log *applyLog, id NodeID, want int, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if log.count(id) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("node %d: applied %d entries, want at least %d within %s", id, log.count(id), want, deadline)
}

// waitForLast waits for a particular command rather than an entry count. A crash test can
// legitimately commit its leader-discovery probe before the crash, so a count alone does
// not establish that the later post-failure command has reached the state machine.
func waitForLast(t *testing.T, log *applyLog, id NodeID, want string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if string(log.last(id)) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("node %d: most recent applied entry is %q, want %q within %s", id, log.last(id), want, deadline)
}

// TestHostTCPClusterElectsAndReplicates proves the production transport, not just the pure
// protocol: three Hosts on real loopback sockets, each backed by its own on-disk storage
// engine via Persister, must elect a leader and replicate a proposal's exact bytes to every
// follower's Apply callback.
func TestHostTCPClusterElectsAndReplicates(t *testing.T) {
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

	if err := proposeToLeader(hosts, []byte("hello over real tcp"), 20*time.Second); err != nil {
		t.Fatalf("no leader accepted a proposal within the deadline: %v", err)
	}

	for _, id := range ids {
		waitForCount(t, log, id, 1, 15*time.Second)
		if got := log.last(id); string(got) != "hello over real tcp" {
			t.Fatalf("node %d: applied %q, want %q", id, got, "hello over real tcp")
		}
	}
}

// TestHostReadIndexRequiresAndObtainsQuorum proves the conservative read barrier commits
// only after a live quorum confirms the leader, while keeping its internal no-op out of
// the hosted application state machine.
func TestHostReadIndexRequiresAndObtainsQuorum(t *testing.T) {
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
		hosts = append(hosts, startTestHost(t, id, ids, addrs[id], peers, log.applierFor(id)))
	}
	stop := make(chan struct{})
	wg := driveHosts(hosts, 10*time.Millisecond, stop)
	defer func() {
		close(stop)
		wg.Wait()
		for _, host := range hosts {
			host.close(t)
		}
	}()
	if err := proposeToLeader(hosts, []byte("visible data"), 20*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		waitForCount(t, log, id, 1, 15*time.Second)
	}
	var leader *testHost
	for _, host := range hosts {
		if role, _ := host.host.Status(); role == Leader {
			leader = host
			break
		}
	}
	if leader == nil {
		t.Fatal("leader disappeared before read index")
	}
	if _, err := leader.host.ReadIndex(5 * time.Second); err != nil {
		t.Fatalf("quorum read barrier: %v", err)
	}
	// The data command is the only client-visible application entry. The barrier's own
	// committed log entry must not arrive at an index's or range's Apply callback.
	for _, id := range ids {
		if got := log.count(id); got != 1 {
			t.Fatalf("node %d applied %d user commands after barrier, want 1", id, got)
		}
	}
	for _, host := range hosts {
		if host != leader {
			if err := host.host.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := leader.host.ReadIndex(150 * time.Millisecond); err == nil {
		t.Fatal("isolated leader served a quorum-confirmed read")
	}
}

// TestHostTCPClusterSurvivesLeaderFailure kills whichever node is currently leader --
// closing its socket the way a crashed process would -- and confirms the surviving
// majority elects a new leader over real TCP and keeps committing. This is the actual
// distributed-systems claim the project makes; without this test that claim was untested.
func TestHostTCPClusterSurvivesLeaderFailure(t *testing.T) {
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
		hosts = append(hosts, startTestHost(t, id, ids, addrs[id], peers, log.applierFor(id)))
	}

	stop := make(chan struct{})
	wg := driveHosts(hosts, 10*time.Millisecond, stop)

	if err := proposeToLeader(hosts, []byte("before failure"), 20*time.Second); err != nil {
		t.Fatalf("initial proposal failed: %v", err)
	}
	for _, id := range ids {
		waitForCount(t, log, id, 1, 15*time.Second)
	}

	// Find the current leader by trying Propose on each host with a tiny payload marker --
	// whichever one accepts is leader. Discard that extra committed entry from bookkeeping
	// is unnecessary here since we only assert counts increase monotonically below.
	var leader *testHost
	var others []*testHost
	for _, h := range hosts {
		if err := h.host.Propose([]byte("probe")); err == nil {
			leader = h
		} else {
			others = append(others, h)
		}
	}
	if leader == nil {
		t.Fatal("no leader found to fail")
	}

	// Kill the leader the way a real crash would: close its transport so peers see dial
	// failures, not a clean shutdown message.
	if err := leader.host.Close(); err != nil {
		t.Fatalf("closing leader transport: %v", err)
	}
	if err := leader.db.Close(); err != nil {
		t.Fatalf("closing leader storage: %v", err)
	}

	if err := proposeToLeader(others, []byte("after failure"), 25*time.Second); err != nil {
		t.Fatalf("no new leader emerged among survivors within the deadline: %v", err)
	}
	// "before failure" was durably committed to all three nodes before the kill; "probe"
	// was sent but the leader was closed before its commit could be confirmed, so whether
	// it lands is legitimately undefined (the new leader may or may not finish committing
	// it while re-establishing its own term) and asserting on it would be flaky. What must
	// hold, deterministically, is that the surviving majority keeps making progress: at
	// least the pre-failure entry plus the post-failure one.
	for _, h := range others {
		waitForLast(t, log, h.id, "after failure", 15*time.Second)
	}

	close(stop)
	wg.Wait()
	for _, h := range others {
		h.close(t)
	}
}
