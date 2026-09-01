package raft

import (
	"errors"
	"sync"
)

// HostConfig supplies the impure adapters around one pure Raft Node. Apply must be
// idempotent across restart because a committed entry can be replayed after durable commit
// but before the previous process reached its state-machine callback.
type HostConfig struct {
	Raft          Config
	ListenAddress string
	Peers         map[NodeID]string
	Persister     *Persister
	Apply         func(Entry) error
}

// Host owns the serialization point between a Raft state machine and real I/O. It has no
// protocol decisions of its own: every state transition still happens in Node.Step/Tick.
type Host struct {
	mu        sync.Mutex
	node      Node
	persister *Persister
	transport *TCPTransport
	apply     func(Entry) error
}

// NewHost starts a TCP listener and restores its node before accepting messages. The
// recovery-before-listen order prevents a rebooted node from answering an RPC using an
// empty term or log after durable state already exists on disk.
func NewHost(config HostConfig) (*Host, error) {
	if config.Persister == nil || config.Apply == nil {
		return nil, errors.New("raft: host persister and apply callback required")
	}
	node, err := RecoverNode(config.Raft, config.Persister)
	if err != nil {
		return nil, err
	}
	host := &Host{node: node, persister: config.Persister, apply: config.Apply}
	transport, err := ListenTCP(config.Raft.ID, config.ListenAddress, config.Peers, host.Step)
	if err != nil {
		return nil, err
	}
	host.transport = transport
	return host, nil
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
		if err := h.apply(entry); err != nil {
			return err
		}
	}
	h.node.Advance()
	return nil
}
