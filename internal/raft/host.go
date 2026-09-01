package raft

import (
	"errors"
	"sync"
	"time"
)

var readBarrierCommand = []byte("consensa/raft/read-barrier/v1")

// HostConfig supplies the impure adapters around one pure Raft Node. Apply must be
// idempotent across restart because a committed entry can be replayed after durable commit
// but before the previous process reached its state-machine callback.
type HostConfig struct {
	Raft          Config
	ListenAddress string
	Peers         map[NodeID]string
	Persister     *Persister
	Apply         func(Entry) error
	// Transport, if set, is used instead of NewHost dialing its own dedicated TCP
	// listener -- this is what lets multiple ranges' Hosts on the same node share one
	// real listener through a *MultiplexedTransport (see multiplex.go) instead of each
	// opening its own socket. ListenAddress and Peers are ignored when Transport is set;
	// the caller already resolved addressing when it built the shared transport.
	Transport Transport
}

// Host owns the serialization point between a Raft state machine and real I/O. It has no
// protocol decisions of its own: every state transition still happens in Node.Step/Tick.
type Host struct {
	mu        sync.Mutex
	node      Node
	persister *Persister
	transport Transport
	apply     func(Entry) error
	progress  chan struct{}
}

// NewHost restores its node before accepting messages, then either starts a dedicated TCP
// listener or attaches to a caller-supplied shared Transport. The recovery-before-listen
// order prevents a rebooted node from answering an RPC using an empty term or log after
// durable state already exists on disk.
func NewHost(config HostConfig) (*Host, error) {
	if config.Persister == nil || config.Apply == nil {
		return nil, errors.New("raft: host persister and apply callback required")
	}
	// The pure Node accepts its exact configured timeout so simulator tests can control
	// every logical tick. Real Hosts need the other half of Raft's election-timer rule:
	// replicas must not all campaign on the same wall-clock tick. A random timeout would
	// make a restart's behavior harder to reproduce, so derive a small, stable offset
	// from the persisted replica identity instead. All replicas still use the same
	// configured base, while the first campaign is separated by at least one tick.
	raftConfig := config.Raft
	raftConfig.ElectionTick += hostElectionStagger(raftConfig.ID, raftConfig.ElectionTick)
	node, err := RecoverNode(raftConfig, config.Persister)
	if err != nil {
		return nil, err
	}
	host := &Host{node: node, persister: config.Persister, apply: config.Apply, progress: make(chan struct{})}
	if config.Transport != nil {
		host.transport = config.Transport
		return host, nil
	}
	transport, err := ListenTCP(config.Raft.ID, config.ListenAddress, config.Peers, host.Step)
	if err != nil {
		return nil, err
	}
	host.transport = transport
	return host, nil
}

// hostElectionStagger returns a deterministic offset strictly smaller than half the
// configured timeout. It is deliberately an adapter concern: NewNode remains a pure,
// exactly-ticked state machine for the simulator, whereas a real Host needs the
// wall-clock election de-synchronization Raft relies on in production.
func hostElectionStagger(id NodeID, base int) int {
	span := base / 2
	if span < 1 {
		span = 1
	}
	return 1 + int((uint64(id)-1)%uint64(span))
}

// Addr returns the peer address assigned to this host.
func (h *Host) Addr() string { return h.transport.Addr().String() }

// Status reports this replica's own role and term. Like the rest of Raft, this is this
// node's local view -- it can be stale relative to the cluster during a partition or a
// pending election, which is expected and not itself an error condition.
func (h *Host) Status() (Role, uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.node.Status()
}

// Tick advances election/heartbeat time and executes the emitted Ready work.
func (h *Host) Tick() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.node.Tick()
	return h.driveLocked()
}

// Step accepts one inbound transport message and executes all resulting work atomically
// with respect to Tick and Propose. TCP handlers may call this concurrently.
func (h *Host) Step(message Message) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.node.Step(message); err != nil {
		return err
	}
	return h.driveLocked()
}

// Propose submits a command to this host if it is the current leader.
func (h *Host) Propose(data []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.node.Propose(data); err != nil {
		return err
	}
	return h.driveLocked()
}

// ReadIndex confirms this leader still has a quorum before a linearizable read. It
// replicates a reserved no-op and waits until that entry is locally applied; committing it
// requires a current quorum, so an isolated former leader times out instead of serving a
// response that could violate real-time order. This is deliberately a conservative
// read-barrier implementation: the later heartbeat-context optimization can remove the
// log entry without weakening the safety argument.
func (h *Host) ReadIndex(timeout time.Duration) (uint64, error) {
	h.mu.Lock()
	if role, _ := h.node.Status(); role != Leader {
		h.mu.Unlock()
		return 0, errors.New("raft: read index requires leader")
	}
	if err := h.node.Propose(readBarrierCommand); err != nil {
		h.mu.Unlock()
		return 0, err
	}
	target := h.node.(*node).log.lastIndex()
	if err := h.driveLocked(); err != nil {
		h.mu.Unlock()
		return 0, err
	}
	progress := h.progress
	h.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-progress:
			h.mu.Lock()
			applied := h.node.(*node).log.applied
			role, _ := h.node.Status()
			next := h.progress
			h.mu.Unlock()
			if applied >= target && role == Leader {
				return target, nil
			}
			progress = next
		case <-timer.C:
			return 0, errors.New("raft: read index quorum confirmation timed out")
		}
	}
}

// Close stops the peer listener. It does not close the caller-owned storage engine.
func (h *Host) Close() error { return h.transport.Close() }

func (h *Host) driveLocked() error {
	ready := h.node.Ready()
	if len(ready.Entries) == 0 && len(ready.Messages) == 0 && len(ready.CommittedEntries) == 0 && ready.Snapshot.Index == 0 && ready.HardState == h.node.(*node).lastHard {
		return nil
	}
	// Raft's durable boundary: persist term/vote/log before a peer can observe any
	// message that relies on it. Do not refactor this ordering into asynchronous calls.
	if err := h.persister.Persist(ready); err != nil {
		return err
	}
	// A send failure to one peer must not block progress toward the others: Raft is
	// designed to tolerate a minority of unreachable nodes, and any message dropped here
	// gets naturally retried on the next heartbeat once the node is Ready() again. Returning
	// early on the first dial failure -- the previous behavior -- would let a single dead
	// peer wedge replication to a live majority too, since Advance() below would never run
	// and the same stale Ready (including the doomed message) would be re-emitted forever.
	for _, message := range ready.Messages {
		_ = h.transport.Send(message)
	}
	for _, entry := range ready.CommittedEntries {
		if string(entry.Data) != string(readBarrierCommand) {
			if err := h.apply(entry); err != nil {
				return err
			}
		}
	}
	h.node.Advance()
	close(h.progress)
	h.progress = make(chan struct{})
	return nil
}
