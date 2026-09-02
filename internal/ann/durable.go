package ann

import (
	"sync"

	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/storage"
	"github.com/ashraf/consensa/internal/vector"
)

// DurableNode is one HNSW replica backed by a real Raft transport and an on-disk storage
// engine, in contrast to ReplicatedIndex's in-memory Cluster harness. Its durability is not
// implemented by snapshotting the graph: Persister already writes every committed Raft
// entry to the LSM (see internal/raft/storage.go), and RecoverNode restores committed-but-
// not-yet-applied entries into a fresh Ready() on restart. DurableNode's Apply callback is
// exactly HNSW.ApplyMutation, so a killed-and-restarted node recovers its whole graph by
// replaying its own persisted Raft log — no HNSW-specific persistence code is required.
type DurableNode struct {
	mu    sync.RWMutex // guards index: Host may invoke apply concurrently with a caller's Search
	host  *raft.Host
	db    *storage.DB
	index *HNSW

	appliedMu sync.Mutex
	applied   int
}

// DurableNodeConfig names everything one replica needs: its own storage directory (reused
// verbatim across a restart), the fixed Raft group membership, its own listen address, and
// the addresses of its peers.
type DurableNodeConfig struct {
	ID             raft.NodeID
	GroupPeers     []raft.NodeID
	Learners       []raft.NodeID
	ListenAddress  string
	TransportPeers map[raft.NodeID]string
	// Transport optionally attaches this group to a logical view of a shared listener.
	// When nil, NewDurableNode creates its historical dedicated TCP listener.
	Transport     raft.Transport
	StorageDir    string
	Index         Config
	ElectionTick  int
	HeartbeatTick int
}

// NewDurableNode opens (or recovers) the on-disk engine, builds a fresh HNSW graph, and
// starts the Raft host with Apply wired directly to that graph. If StorageDir already
// holds a committed log from a previous run, the very first Ready() cycle replays it into
// the graph before this call returns -- so the returned node is caught up, not empty.
func NewDurableNode(cfg DurableNodeConfig) (*DurableNode, error) {
	db, err := storage.Open(storage.Options{Dir: cfg.StorageDir, SyncEvery: 1})
	if err != nil {
		return nil, err
	}
	index, err := NewHNSW(cfg.Index)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	d := &DurableNode{db: db, index: index}

	electionTick, heartbeatTick := cfg.ElectionTick, cfg.HeartbeatTick
	if electionTick == 0 {
		electionTick = 10
	}
	if heartbeatTick == 0 {
		heartbeatTick = 2
	}

	host, err := raft.NewHost(raft.HostConfig{
		Raft:          raft.Config{ID: cfg.ID, Peers: cfg.GroupPeers, Learners: cfg.Learners, ElectionTick: electionTick, HeartbeatTick: heartbeatTick},
		ListenAddress: cfg.ListenAddress,
		Peers:         cfg.TransportPeers,
		Transport:     cfg.Transport,
		Persister:     raft.NewPersister(db),
		Apply:         d.apply,
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	d.host = host
	return d, nil
}

func (d *DurableNode) apply(entry raft.Entry) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.index.ApplyMutation(entry.Data); err != nil {
		return err
	}
	d.appliedMu.Lock()
	d.applied++
	d.appliedMu.Unlock()
	return nil
}

// AppliedCount reports how many mutations this replica has applied so far -- since Propose
// does not block for commit (real replication over TCP is asynchronous by nature), callers
// that need to observe a write land poll this rather than assuming Insert/Delete succeeded.
func (d *DurableNode) AppliedCount() int {
	d.appliedMu.Lock()
	defer d.appliedMu.Unlock()
	return d.applied
}

// Tick drives this replica's election/heartbeat clock and processes any resulting work.
// The caller is responsible for calling it on a regular interval; see driveHosts in
// internal/raft/host_test.go for the pattern this mirrors.
func (d *DurableNode) Tick() error { return d.host.Tick() }

// ProposeConfChange changes voters among the transport-known peer universe. A new peer
// must start as a caught-up learner before its promotion is proposed to the current leader.
func (d *DurableNode) ProposeConfChange(voters, learners []raft.NodeID) error {
	return d.host.ProposeConfChange(voters, learners)
}

// Insert proposes an encoded mutation to this replica. It only succeeds if this replica is
// currently the Raft leader, matching Host.Propose's contract -- callers without a known
// leader should retry across replicas the way internal/raft/host_test.go's
// proposeToLeader does.
func (d *DurableNode) Insert(id string, v vector.Vector) error {
	data, err := EncodeMutation(id, v)
	if err != nil {
		return err
	}
	return d.host.Propose(data)
}

// Delete proposes a deterministic deletion mutation. See Insert for the leader contract.
func (d *DurableNode) Delete(id string) error {
	data, err := EncodeDeleteMutation(id)
	if err != nil {
		return err
	}
	return d.host.Propose(data)
}

// Search reads from this replica's local graph. It does not consult Raft leadership: once a
// mutation is applied, every replica's graph is equal by construction (Mutation.go's
// determinism guarantee), so any replica -- not only the leader -- may safely serve reads.
func (d *DurableNode) Search(query vector.Vector, k, ef int) ([]Result, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.index.Search(query, k, ef)
}

// Validate checks a vector against this replica's graph configuration.
func (d *DurableNode) Validate(v vector.Vector) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.index.Validate(v)
}

// GetVector returns an exact vector already applied to this replica's graph. It is safe
// for recovery-time BatchGet because NewDurableNode rebuilds the graph from durable Raft
// state before returning to its caller.
func (d *DurableNode) GetVector(id string) (vector.Vector, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.index.GetVector(id)
}

// Addr returns this replica's bound transport address, useful when ListenAddress was
// "127.0.0.1:0" and the OS assigned the actual port.
func (d *DurableNode) Addr() string { return d.host.Addr() }

// Status reports this replica's own view of its Raft role and term, matching the shape
// server.Service.Status already expects via a duck-typed interface. The returned id is
// this replica's own recognized leader (0 if unknown), from Host.Leader -- not a
// guarantee of cluster-wide accuracy, just this replica's local, possibly-stale view.
func (d *DurableNode) Status() (id raft.NodeID, term uint64, isLeader bool) {
	role, term := d.host.Status()
	return d.host.Leader(), term, role == raft.Leader
}

// Leader returns this replica's own recognized leader for the group, or 0 if unknown.
func (d *DurableNode) Leader() raft.NodeID { return d.host.Leader() }

// TransferLeadershipTo asks this replica, if it is currently leader, to hand leadership
// to a specific caught-up peer -- see raft.Host.TransferLeadershipTo's doc comment.
func (d *DurableNode) TransferLeadershipTo(to raft.NodeID) error {
	return d.host.TransferLeadershipTo(to)
}

// Close stops the transport and closes the storage engine. It does not delete StorageDir:
// a caller that reopens NewDurableNode against the same directory afterward is exactly the
// crash-restart-and-recover scenario this type exists to support.
func (d *DurableNode) Close() error {
	if err := d.host.Close(); err != nil {
		return err
	}
	return d.db.Close()
}
