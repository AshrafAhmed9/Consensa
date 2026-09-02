package raft

import "errors"

// Cluster is a deterministic in-memory assembly of one Raft group. It is a test/runtime
// harness, not a network transport: callers explicitly Tick and Deliver, so all scheduling
// decisions remain observable and replayable.
type Cluster struct {
	nodes   map[NodeID]Node
	applied map[NodeID][][]byte
}

// NewCluster creates identical replicas for a fixed voter set.
func NewCluster(ids []NodeID) (*Cluster, error) {
	if len(ids) == 0 {
		return nil, errors.New("raft: empty cluster")
	}
	c := &Cluster{nodes: map[NodeID]Node{}, applied: map[NodeID][][]byte{}}
	for position, id := range ids {
		// Staggered logical timeouts are the deterministic analogue of Raft's randomized
		// election timers: they prevent perpetual simultaneous candidacies in this harness.
		n, err := NewNode(Config{ID: id, Peers: ids, ElectionTick: 3 + position, HeartbeatTick: 1})
		if err != nil {
			return nil, err
		}
		c.nodes[id] = n
	}
	return c, nil
}

// Tick advances every replica one logical tick then delivers all resulting RPCs to quiescence.
func (c *Cluster) Tick() error {
	for _, n := range c.nodes {
		n.Tick()
	}
	return c.Deliver()
}

// Deliver executes queued Ready cycles until no replica emits another message. It models a
// reliable fully-connected network; the simulator provides fault injection around this loop.
func (c *Cluster) Deliver() error {
	return c.DeliverFiltered(func(Message) bool { return true })
}

// DeliverFiltered runs a delivery cycle while predicate decides which messages cross the
// simulated network. Returning false drops a message permanently, which models partitions
// without adding goroutines or wall-clock timing to the protocol.
func (c *Cluster) DeliverFiltered(deliver func(Message) bool) error {
	for progress := true; progress; {
		progress = false
		for id, n := range c.nodes {
			ready := n.Ready()
			for _, message := range ready.Messages {
				if !deliver(message) {
					continue
				}
				target := c.nodes[message.To]
				if target == nil {
					return errors.New("raft: unknown target")
				}
				if err := target.Step(message); err != nil {
					return err
				}
				progress = true
			}
			for _, entry := range ready.CommittedEntries {
				c.applied[id] = append(c.applied[id], append([]byte(nil), entry.Data...))
			}
			if len(ready.Messages) > 0 || len(ready.CommittedEntries) > 0 || len(ready.Entries) > 0 {
				n.Advance()
			}
		}
	}
	return nil
}

// Propose submits through a specified leader and delivers the result across the group.
func (c *Cluster) Propose(leader NodeID, data []byte) error {
	n := c.nodes[leader]
	if n == nil {
		return errors.New("raft: unknown leader")
	}
	if err := n.Propose(data); err != nil {
		return err
	}
	return c.Deliver()
}

// TickFiltered advances every replica one logical tick, then delivers using deliver instead
// of Tick's always-connected delivery. This is what an external fault-injection driver (see
// cmd/torture) needs and could not otherwise get: Tick alone can only ever model a fully
// reliable network from outside this package, since c.nodes is unexported and Deliver's
// predicate defaults to "always true."
func (c *Cluster) TickFiltered(deliver func(Message) bool) error {
	for _, n := range c.nodes {
		n.Tick()
	}
	return c.DeliverFiltered(deliver)
}

// ProposeFiltered submits through a specified leader and delivers using deliver instead of
// Propose's always-connected delivery, so an external driver can test what happens to a
// proposal issued during a partition or into an isolated leader.
func (c *Cluster) ProposeFiltered(leader NodeID, data []byte, deliver func(Message) bool) error {
	n := c.nodes[leader]
	if n == nil {
		return errors.New("raft: unknown leader")
	}
	if err := n.Propose(data); err != nil {
		return err
	}
	return c.DeliverFiltered(deliver)
}

// TransferLeadershipTo submits a leadership transfer through the specified current
// leader and delivers the resulting MsgTimeoutNow/election exchange across the group,
// mirroring Propose's shape for the equivalent operation.
func (c *Cluster) TransferLeadershipTo(from, to NodeID) error {
	n := c.nodes[from]
	if n == nil {
		return errors.New("raft: unknown leader")
	}
	if err := n.TransferLeadershipTo(to); err != nil {
		return err
	}
	return c.Deliver()
}

// Leader returns the current elected leader, if the group has completed an election.
//
// More than one node can believe itself Leader at once -- a node isolated by a partition
// keeps its stale role and term until it reconnects and hears a higher term, which is
// exactly the "zombie leader" scenario Raft's safety properties tolerate rather than
// prevent. Breaking the tie by highest term (rather than by c.nodes' undefined map
// iteration order, which the previous version did) picks the node a real client would
// actually observe as current: the isolated node's term cannot exceed a live quorum's,
// so this can never surface the stale side once a real replacement has been elected.
func (c *Cluster) Leader() (NodeID, bool) {
	var (
		leader     NodeID
		leaderTerm uint64
		found      bool
	)
	for id, replica := range c.nodes {
		n := replica.(*node)
		if n.role != Leader {
			continue
		}
		if !found || n.term > leaderTerm {
			leader, leaderTerm, found = id, n.term, true
		}
	}
	return leader, found
}

// Status reports the elected leader and its term for administrative surfaces.
func (c *Cluster) Status() (NodeID, uint64, bool) {
	leader, ok := c.Leader()
	if !ok {
		return 0, 0, false
	}
	return leader, c.nodes[leader].(*node).term, true
}

// Applied returns a copy of commands applied by one replica.
func (c *Cluster) Applied(id NodeID) [][]byte {
	out := make([][]byte, len(c.applied[id]))
	for i, v := range c.applied[id] {
		out[i] = append([]byte(nil), v...)
	}
	return out
}
