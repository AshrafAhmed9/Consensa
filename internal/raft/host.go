package raft

import (
	"errors"
	"sort"
	"sync"
)

// HostConfig is everything a Host needs on top of a Raft Node.
type HostConfig struct {
	Raft          Config
	ListenAddress string
	Peers         map[NodeID]string // address of every other node
	Persister     *Persister
	// Apply is called once for each committed entry, in order. After a restart it can be
	// called again for entries it already saw, so it must cope with repeats.
	Apply func(Entry) error
}

// Host connects a Raft Node to the real world: it saves state to disk, sends messages
// over TCP and applies committed entries. All protocol decisions stay in the Node. One
// mutex makes Tick, Step and Propose run one at a time.
type Host struct {
	mu        sync.Mutex
	node      Node
	persister *Persister
	transport *TCPTransport
	apply     func(Entry) error
}

// NewHost loads any saved state first and only then starts listening, so a restarted
// node never answers a message with an empty term or log.
func NewHost(config HostConfig) (*Host, error) {
	if config.Persister == nil || config.Apply == nil {
		return nil, errors.New("raft: host persister and apply callback required")
	}
	// Give each node a slightly different election timeout so they don't all start an
	// election on the same tick. The offset is fixed (not random) so a restart behaves
	// the same way.
	raftConfig := config.Raft
	raftConfig.ElectionTick += hostElectionStagger(raftConfig.ID, raftConfig.Peers, raftConfig.ElectionTick)
	node, err := RecoverNode(raftConfig, config.Persister)
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

// electionStaggerSpread widens the gap between the nodes' election timeouts, so the node
// ranked first reliably starts its election first.
const electionStaggerSpread = 4

// hostElectionStagger returns how many extra ticks this node waits before it campaigns.
// It depends on the node's rank among the sorted peer IDs, so the lowest-ranked node
// campaigns first. Nodes are ranked rather than using raw IDs so sparse IDs still work.
func hostElectionStagger(id NodeID, peers []NodeID, base int) int {
	if base < 1 || len(peers) == 0 {
		return 1
	}
	ordered := append([]NodeID(nil), peers...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for position, peer := range ordered {
		if peer == id {
			return 1 + position*base*electionStaggerSpread/len(ordered)
		}
	}
	// NewNode rejects an ID that is not in peers; return here and let it report that.
	return 1
}

// Addr returns the peer address assigned to this host.
func (h *Host) Addr() string { return h.transport.Addr().String() }

// Status returns this node's role and term. It is a local view and can be out of date.
func (h *Host) Status() (Role, uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.node.Status()
}

// Leader returns who this node thinks the leader is, or 0 if unknown.
func (h *Host) Leader() NodeID {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.node.Leader()
}

// Tick advances the clocks and then does any resulting work.
func (h *Host) Tick() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.node.Tick()
	return h.driveLocked()
}

// Step handles one incoming message. TCP handlers call it from many goroutines.
func (h *Host) Step(message Message) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.node.Step(message); err != nil {
		return err
	}
	return h.driveLocked()
}

// Propose submits a command. It fails unless this node is the leader.
func (h *Host) Propose(data []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.node.Propose(data); err != nil {
		return err
	}
	return h.driveLocked()
}

// ProposeConfChange starts a change of voters (see confchange.go). Leader only.
func (h *Host) ProposeConfChange(newVoters, newLearners []NodeID) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.node.ProposeConfChange(newVoters, newLearners); err != nil {
		return err
	}
	return h.driveLocked()
}

// AddKnownPeer tells the node that id exists, so it can be named in a config change.
// Call it on every node, together with AddPeer.
func (h *Host) AddKnownPeer(id NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.node.AddKnownPeer(id)
}

// AddPeer teaches the transport the address of a new node, so messages to it can be
// delivered. Call it on every node before the config change that adds the new node.
func (h *Host) AddPeer(id NodeID, address string) {
	h.transport.AddPeer(id, address)
}

// Close stops the peer listener. It does not close the caller-owned storage engine.
func (h *Host) Close() error { return h.transport.Close() }

// driveLocked does the work the node has queued up: save to disk, send messages, apply
// committed entries, then tell the node it is done. The caller must hold h.mu.
func (h *Host) driveLocked() error {
	ready := h.node.Ready()
	if len(ready.Entries) == 0 && len(ready.Messages) == 0 && len(ready.CommittedEntries) == 0 && ready.HardState == h.node.(*node).lastHard {
		return nil // nothing to do
	}
	// Save before sending: a peer must never see a message that depends on state we
	// could lose in a crash (for example, a vote we then forget).
	if err := h.persister.Persist(ready); err != nil {
		return err
	}
	// A failed send to one peer must not stop the rest. The message is lost, but Raft
	// resends on the next heartbeat. Stopping here would let one dead peer block
	// replication to the healthy majority.
	for _, message := range ready.Messages {
		_ = h.transport.Send(message)
	}
	for _, entry := range ready.CommittedEntries {
		if !isRaftInternalData(entry.Data) {
			if err := h.apply(entry); err != nil {
				return err
			}
		}
	}
	h.node.Advance()
	return nil
}
