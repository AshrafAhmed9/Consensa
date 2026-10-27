package raft

import "errors"

// Cluster is a whole Raft group running in one process, with messages passed by direct
// function calls instead of a network. Nothing runs by itself: the caller calls Tick and
// Deliver, so a test run is repeatable. Tests and the torture tool use it to inject faults.
type Cluster struct {
	nodes   map[NodeID]Node
	applied map[NodeID][][]byte
}

// NewCluster creates one node per ID, all voters.
func NewCluster(ids []NodeID) (*Cluster, error) {
	if len(ids) == 0 {
		return nil, errors.New("raft: empty cluster")
	}
	c := &Cluster{nodes: map[NodeID]Node{}, applied: map[NodeID][][]byte{}}
	for position, id := range ids {
		// Each node gets a slightly different election timeout so they don't all campaign at once.
		n, err := NewNode(Config{ID: id, Peers: ids, ElectionTick: 3 + position, HeartbeatTick: 1})
		if err != nil {
			return nil, err
		}
		c.nodes[id] = n
	}
	return c, nil
}

// Tick advances every node by one tick, then delivers messages until nothing is left to send.
func (c *Cluster) Tick() error {
	for _, n := range c.nodes {
		n.Tick()
	}
	return c.Deliver()
}

// Deliver passes messages between nodes until none are left, as on a perfect network.
func (c *Cluster) Deliver() error {
	return c.DeliverFiltered(func(Message) bool { return true })
}

// DeliverFiltered is Deliver, but deliver decides which messages get through. A message
// it rejects is dropped for good, which is how tests model a network partition.
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

// Propose submits data through the given leader and delivers the resulting messages.
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

// TickFiltered is Tick with a message filter (see DeliverFiltered).
func (c *Cluster) TickFiltered(deliver func(Message) bool) error {
	for _, n := range c.nodes {
		n.Tick()
	}
	return c.DeliverFiltered(deliver)
}

// ProposeFiltered is Propose with a message filter (see DeliverFiltered).
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

// Leader returns the node that is currently leader.
//
// After a partition, an isolated old leader can still think it is leader. If several
// nodes claim leadership, the one with the highest term is the real one, because the
// isolated node's term cannot pass the majority's.
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

// Applied returns the commands a node has applied, in order.
func (c *Cluster) Applied(id NodeID) [][]byte {
	out := make([][]byte, len(c.applied[id]))
	for i, v := range c.applied[id] {
		out[i] = append([]byte(nil), v...)
	}
	return out
}
