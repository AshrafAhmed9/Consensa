package raft

import (
	"errors"
	"sort"
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

// inboundTransport is implemented by a logical view of a shared transport. Host installs
// its Step handler itself once construction has succeeded, keeping the Register → Host →
// SetHandler handshake out of every higher-level range constructor.
type inboundTransport interface {
	Transport
	SetHandler(func(Message) error)
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
	raftConfig.ElectionTick += hostElectionStagger(raftConfig.ID, raftConfig.Peers, raftConfig.ElectionTick)
	node, err := RecoverNode(raftConfig, config.Persister)
	if err != nil {
		return nil, err
	}
	host := &Host{node: node, persister: config.Persister, apply: config.Apply, progress: make(chan struct{})}
	if config.Transport != nil {
		host.transport = config.Transport
		if transport, ok := config.Transport.(inboundTransport); ok {
			transport.SetHandler(host.Step)
		}
		return host, nil
	}
	transport, err := ListenTCP(config.Raft.ID, config.ListenAddress, config.Peers, host.Step)
	if err != nil {
		return nil, err
	}
	host.transport = transport
	return host, nil
}

// electionStaggerSpread widens the gap hostElectionStagger puts between consecutively
// ranked peers, beyond the raw [base, 2*base) range the position-based formula alone
// would produce. A process that hosts several independently-elected Raft groups sharing
// one deployment (cmd/consensa's vector index plus its KV ranges, all built from the same
// peer list) relies on the SAME node winning every one of those groups' elections, since
// nothing forwards a request to a different process's leader (docs/notes/05-api.md). The
// position-based formula alone gives the favored (lowest-ranked) node only a fractional
// head start over the next-ranked node, which real network delivery jitter between
// otherwise-identical, independently-timed groups can occasionally overcome -- producing
// a real, observed failure (docs/bugs/003) where different processes end up leading
// different co-located groups. Multiplying the spread gives the favored node a much
// larger, harder-to-overcome head start without changing anything about which node is
// favored or the safety of the election protocol itself -- Raft's safety never depends on
// the exact size of this gap, only on some real desynchronization existing at all.
const electionStaggerSpread = 4

// hostElectionStagger returns a deterministic offset distributed across one base timeout.
// It is deliberately an adapter concern: NewNode remains a pure, exactly-ticked state
// machine for the simulator, whereas a real Host needs the wall-clock election
// de-synchronization Raft relies on in production. The sorted membership rank, rather
// than a raw node ID, keeps the bound stable for sparse IDs and gives independently
// hosted static ranges the same deterministic first leader.
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
	// NewNode will reject a local ID absent from peers. Keep this helper total so the
	// caller still returns that useful validation error instead of panicking first.
	return 1
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

// Leader reports this replica's own, possibly-stale view of who currently leads the
// group, or 0 if unknown -- see Node.Leader's doc comment.
func (h *Host) Leader() NodeID {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.node.Leader()
}

// TransferLeadershipTo asks this host, if it is currently leader, to hand leadership to a
// specific caught-up voting peer -- see Node.TransferLeadershipTo's doc comment for the
// safety argument. This is docs/bugs/003's real fix, not the earlier mitigation
// (electionStaggerSpread): a process that finds itself leading some but not all of its
// co-located Raft groups can now actively request leadership of the groups it is missing,
// rather than only hoping a wider election-timer bias prevents the split from ever
// occurring in the first place.
func (h *Host) TransferLeadershipTo(to NodeID) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.node.TransferLeadershipTo(to); err != nil {
		return err
	}
	return h.driveLocked()
}

// ConfState returns the membership metadata that must be included with a snapshot made
// from this host. It is separate from application snapshot bytes because Raft itself,
// not the state machine, owns quorum configuration.
func (h *Host) ConfState() ConfState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.node.ConfState()
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

// ProposeConfChange begins a joint-consensus membership transition on this host's node --
// see Node.ProposeConfChange's own doc comment for the safety argument and scope. This is
// the primitive only: no caller in this codebase (kv.DurableRange, cmd/consensa) invokes
// it yet, the same built-but-unwired-into-a-real-deployment gap this project has closed
// for other primitives before -- closing it here would additionally need a way for
// kv.DurableRange callers to learn about and route to a newly-promoted or newly-removed
// replica, which is real, separate work.
func (h *Host) ProposeConfChange(newVoters, newLearners []NodeID) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.node.ProposeConfChange(newVoters, newLearners); err != nil {
		return err
	}
	return h.driveLocked()
}

// AddKnownPeer extends this Host's Node's local peer universe to include id -- the other
// half of provisioning a genuinely new process, alongside AddPeer below (which handles
// the transport address; this handles Raft's own ProposeConfChange eligibility check).
// See Node.AddKnownPeer's own doc comment for why both are local, per-replica operations
// that must be called on every existing replica, not just the leader.
func (h *Host) AddKnownPeer(id NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.node.AddKnownPeer(id)
}

// AddPeer registers address for peer id on this Host's underlying transport, so a message
// to a NodeID just added via ProposeConfChange (e.g. a genuinely new process nothing in
// this deployment has ever addressed before) can actually be delivered instead of failing
// with "unknown peer address". This must be called on every existing replica before or
// alongside the ProposeConfChange that introduces the new ID -- ProposeConfChange changes
// Raft's own membership state, which is a separate, orthogonal concern from whether the
// transport layer knows how to reach that ID's address; closing only one half would leave
// a new voter or learner that Raft counts toward quorum but can never actually replicate
// to. Returns an error if this Host's transport doesn't implement raft.PeerRegistrar --
// true only for a caller-supplied Transport (config.Transport) that doesn't support it,
// never for the transports NewHost builds itself (ListenTCP, MultiplexedTransport.Register).
func (h *Host) AddPeer(id NodeID, address string) error {
	registrar, ok := h.transport.(PeerRegistrar)
	if !ok {
		return errors.New("raft: this host's transport does not support adding peers")
	}
	registrar.AddPeer(id, address)
	return nil
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
		if string(entry.Data) != string(readBarrierCommand) && !isRaftInternalData(entry.Data) {
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
