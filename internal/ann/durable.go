package ann

import (
	"sync"

	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/storage"
	"github.com/ashraf/consensa/internal/vector"
)

// DurableNode is one replica of the vector index. It joins three things:
//   - a Raft host, which agrees with the other replicas on the order of inserts and deletes;
//   - an on-disk store, where Raft saves its log;
//   - an HNSW graph, which every committed insert or delete is applied to.
//
// The graph itself is never saved. After a restart the node replays its own Raft log
// into an empty graph and ends up with the same graph as before.
type DurableNode struct {
	mu    sync.RWMutex // guards index: Raft applies entries while searches run
	host  *raft.Host
	db    *storage.DB
	index *HNSW

	appliedMu sync.Mutex
	applied   int // how many entries have been applied to the graph
}

// DurableNodeConfig describes one replica.
type DurableNodeConfig struct {
	ID             raft.NodeID
	GroupPeers     []raft.NodeID          // every node in the group, including this one
	ListenAddress  string                 // where this node accepts Raft messages from peers
	TransportPeers map[raft.NodeID]string // address of every other node
	StorageDir     string                 // directory for the Raft log (reuse it to restart)
	Index          Config                 // HNSW settings
	ElectionTick   int                    // defaults to 10
	HeartbeatTick  int                    // defaults to 2
}

// NewDurableNode opens the store, builds an empty graph and starts the Raft host. If the
// directory already holds a log, the committed entries are replayed into the graph the
// first time the node is ticked, before it talks to any peer.
func NewDurableNode(cfg DurableNodeConfig) (*DurableNode, error) {
	db, err := storage.Open(storage.Options{Dir: cfg.StorageDir}) // Raft syncs once per batch of writes
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
		Raft:          raft.Config{ID: cfg.ID, Peers: cfg.GroupPeers, ElectionTick: electionTick, HeartbeatTick: heartbeatTick},
		ListenAddress: cfg.ListenAddress,
		Peers:         cfg.TransportPeers,
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

// apply runs for every committed Raft entry, in order, and updates the graph.
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

// AppliedCount returns how many entries this node has applied. Insert and Delete return
// as soon as the entry is proposed, not when it is committed, so callers that need to
// see a write take effect (tests, mostly) poll this.
func (d *DurableNode) AppliedCount() int {
	d.appliedMu.Lock()
	defer d.appliedMu.Unlock()
	return d.applied
}

// Tick moves this node's Raft clock forward. Call it regularly (for example every 50ms).
func (d *DurableNode) Tick() error { return d.host.Tick() }

// Insert proposes adding or replacing a vector. It only works on the Raft leader; on any
// other node it returns an error and the caller should try another node.
func (d *DurableNode) Insert(id string, v vector.Vector) error {
	data, err := EncodeMutation(id, v)
	if err != nil {
		return err
	}
	return d.host.Propose(data)
}

// Delete proposes removing a vector. Like Insert, it only works on the leader.
func (d *DurableNode) Delete(id string) error {
	data, err := EncodeDeleteMutation(id)
	if err != nil {
		return err
	}
	return d.host.Propose(data)
}

// Search reads this node's own graph, so any node can answer, not just the leader. Once
// an entry is applied, every replica's graph is identical. A replica that has not applied
// the newest entries yet may return slightly older results.
func (d *DurableNode) Search(query vector.Vector, k, ef int) ([]Result, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.index.Search(query, k, ef)
}

// Validate checks a vector against the graph settings (length, no NaN or infinity).
func (d *DurableNode) Validate(v vector.Vector) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.index.Validate(v)
}

// Addr returns the address this node accepts Raft messages on (useful when the OS
// picked the port).
func (d *DurableNode) Addr() string { return d.host.Addr() }

// Status returns this node's view of the current leader, the current term, and whether
// this node is the leader. It is a local view and can be out of date.
func (d *DurableNode) Status() (leader raft.NodeID, term uint64, isLeader bool) {
	role, term := d.host.Status()
	return d.host.Leader(), term, role == raft.Leader
}

// Close stops the node and closes the store. It keeps the files, so a new node opened on
// the same directory resumes where this one left off.
func (d *DurableNode) Close() error {
	if err := d.host.Close(); err != nil {
		return err
	}
	return d.db.Close()
}
