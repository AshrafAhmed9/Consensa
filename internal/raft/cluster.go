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

// Leader returns the current elected leader, if the group has completed an election.
func (c *Cluster) Leader() (NodeID, bool) {
	for id, replica := range c.nodes {
		if replica.(*node).role == Leader {
			return id, true
		}
	}
	return 0, false
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
