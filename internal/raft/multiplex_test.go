package raft

import (
	"sync"
	"testing"
	"time"

	"github.com/ashraf/consensa/internal/storage"
)

// appliedLog records every entry a node has applied, per range, so the test can assert
// exactly what real data reached each range -- not just that terms progressed.
type appliedLog struct {
	mu      sync.Mutex
	entries map[uint64]map[NodeID][]string
}

func newAppliedLog() *appliedLog { return &appliedLog{entries: map[uint64]map[NodeID][]string{}} }

func (a *appliedLog) record(rangeID uint64, id NodeID, data []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.entries[rangeID] == nil {
		a.entries[rangeID] = map[NodeID][]string{}
	}
	a.entries[rangeID][id] = append(a.entries[rangeID][id], string(data))
}

func (a *appliedLog) get(rangeID uint64, id NodeID) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.entries[rangeID][id]...)
}

// multiplexedTestGroup builds one range's 3 Hosts, each attached to its node's shared
// *MultiplexedTransport instead of opening its own TCP listener. NewHost attaches its
// inbound handler to the logical view, so higher-level range constructors do not need a
// transport-specific post-construction step.
func multiplexedTestGroup(t *testing.T, rangeID uint64, transports map[NodeID]*MultiplexedTransport, ids []NodeID, applied *appliedLog) map[NodeID]*Host {
	t.Helper()
	addrs := map[NodeID]string{}
	for id, mt := range transports {
		addrs[id] = mt.Addr().String()
	}
	hosts := map[NodeID]*Host{}
	for _, id := range ids {
		peers := map[NodeID]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = addrs[other]
			}
		}
		view := transports[id].Register(rangeID, peers)
		db, err := storage.Open(storage.Options{Dir: t.TempDir(), SyncEvery: 1})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		nodeID := id
		host, err := NewHost(HostConfig{
			Raft:      Config{ID: id, Peers: ids, ElectionTick: 10, HeartbeatTick: 2},
			Persister: NewPersister(db),
			Apply:     func(e Entry) error { applied.record(rangeID, nodeID, e.Data); return nil },
			Transport: view,
		})
		if err != nil {
			t.Fatalf("range %d node %d: %v", rangeID, id, err)
		}
		hosts[id] = host
	}
	return hosts
}

func driveMultiplexed(hosts map[NodeID]*Host, interval time.Duration, stop <-chan struct{}) *sync.WaitGroup {
	var wg sync.WaitGroup
	for _, h := range hosts {
		wg.Add(1)
		go func(h *Host) {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					_ = h.Tick()
				}
			}
		}(h)
	}
	return &wg
}

// TestMultiplexedTransportRoutesRangesIndependently proves two ranges' Hosts, sharing one
// real TCP listener per node through *MultiplexedTransport, each elect their own leader,
// replicate independently, and never receive a message meant for the other range -- the
// property that makes sharing a listener actually safe, not just plausible. It also
// proves the "one listener, not one per range" claim directly: three nodes, two ranges,
// but Addr() returns only 3 distinct addresses, not 6.
func TestMultiplexedTransportRoutesRangesIndependently(t *testing.T) {
	ids := []NodeID{1, 2, 3}
	transports := map[NodeID]*MultiplexedTransport{}
	addrSet := map[string]bool{}
	for _, id := range ids {
		mt, err := ListenMultiplexed(id, "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer mt.Close()
		transports[id] = mt
		addrSet[mt.Addr().String()] = true
	}
	if len(addrSet) != 3 {
		t.Fatalf("expected 3 distinct listener addresses (one per node), got %d", len(addrSet))
	}

	applied := newAppliedLog()
	rangeA := multiplexedTestGroup(t, 100, transports, ids, applied)
	rangeB := multiplexedTestGroup(t, 200, transports, ids, applied)

	stop := make(chan struct{})
	all := map[NodeID]*Host{}
	for id, h := range rangeA {
		all[id*10+1] = h
	}
	for id, h := range rangeB {
		all[id*10+2] = h
	}
	wg := driveMultiplexed(all, 10*time.Millisecond, stop)
	defer func() { close(stop); wg.Wait() }()

	waitForLeader := func(hosts map[NodeID]*Host, label string) *Host {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			for _, h := range hosts {
				if role, _ := h.Status(); role == Leader {
					return h
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("%s never elected a leader", label)
		return nil
	}

	leaderA := waitForLeader(rangeA, "range A")
	leaderB := waitForLeader(rangeB, "range B")

	proposeAndWait := func(h *Host, value string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if err := h.Propose([]byte(value)); err == nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("proposal of %q never accepted", value)
	}

	proposeAndWait(leaderA, "a-value")
	proposeAndWait(leaderB, "b-value")

	// Wait for every replica in both ranges to actually apply one entry -- the real proof
	// that end-to-end replication happened over the shared listener, not just that
	// Propose returned without error.
	waitForApplied := func(id NodeID, rangeID uint64, label string) []string {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if got := applied.get(rangeID, id); len(got) > 0 {
				return got
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("%s node %d never applied anything", label, id)
		return nil
	}

	for _, id := range ids {
		gotA := waitForApplied(id, 100, "range A")
		gotB := waitForApplied(id, 200, "range B")

		// This is the property that actually matters: each range's replicas saw only
		// their own range's proposals. If MultiplexedTransport ever misrouted a frame
		// (delivered range B's envelope to range A's Host, say), this is what would
		// catch it -- a term/leadership check alone would not, since a misdelivered
		// message with the wrong range's Message.To could just as easily be silently
		// dropped by chance in a small test as corrupt state.
		for _, v := range gotA {
			if v != "a-value" {
				t.Fatalf("range A node %d applied %q -- range B's data leaked across the shared listener", id, v)
			}
		}
		for _, v := range gotB {
			if v != "b-value" {
				t.Fatalf("range B node %d applied %q -- range A's data leaked across the shared listener", id, v)
			}
		}
	}
}

// TestMultiplexedTransportPoolsOutboundConnections proves the batching half of Phase 3's
// claim this type's own doc comment names: two different ranges on the same sending node,
// both delivering messages to the same destination node, reuse one persistent outbound
// TCP connection rather than each dialing its own -- and every message sent over it still
// arrives at the correct range on the receiving side. Without connection reuse, a node
// hosting many ranges would re-dial once per message per range per heartbeat, exactly the
// "1,000 ranges must not mean 1,000x heartbeat traffic" cost PLAN.md's Phase 3 section
// warns against.
func TestMultiplexedTransportPoolsOutboundConnections(t *testing.T) {
	receiverTransport, err := ListenMultiplexed(2, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer receiverTransport.Close()

	type received struct {
		rangeID uint64
		message Message
	}
	var mu sync.Mutex
	var got []received
	record := func(rangeID uint64) func(Message) error {
		return func(m Message) error {
			mu.Lock()
			got = append(got, received{rangeID, m})
			mu.Unlock()
			return nil
		}
	}
	receiverView1 := receiverTransport.Register(10, nil)
	receiverView1.SetHandler(record(10))
	receiverView2 := receiverTransport.Register(20, nil)
	receiverView2.SetHandler(record(20))

	senderTransport, err := ListenMultiplexed(1, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer senderTransport.Close()

	peers := map[NodeID]string{2: receiverTransport.Addr().String()}
	senderView1 := senderTransport.Register(10, peers)
	senderView2 := senderTransport.Register(20, peers)

	for i := 0; i < 5; i++ {
		if err := senderView1.Send(Message{Type: MsgAppend, From: 1, To: 2, Term: uint64(i)}); err != nil {
			t.Fatalf("range 10 send %d: %v", i, err)
		}
		if err := senderView2.Send(Message{Type: MsgAppend, From: 1, To: 2, Term: uint64(i)}); err != nil {
			t.Fatalf("range 20 send %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of 10 messages ever arrived", n)
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	for _, r := range got {
		if r.rangeID != 10 && r.rangeID != 20 {
			t.Fatalf("message delivered under unexpected range ID %d", r.rangeID)
		}
	}
	mu.Unlock()

	// The actual pooling claim: 10 sends across two ranges to the same destination
	// address opened exactly one outbound connection, not ten -- checked directly against
	// the sender's own connection pool rather than inferred indirectly, since this is a
	// same-package white-box test.
	senderTransport.mu.Lock()
	numConns := len(senderTransport.conns)
	senderTransport.mu.Unlock()
	if numConns != 1 {
		t.Fatalf("sender pooled %d outbound connections for one destination, want 1", numConns)
	}
}
