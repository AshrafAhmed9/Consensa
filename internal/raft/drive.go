package raft

// Driver executes one Ready cycle in the only safe order. It intentionally accepts plain
// callbacks rather than owning a transport or state machine, keeping the Raft algorithm
// deterministic while making the persist-before-send rule difficult to accidentally invert.
type Driver struct {
	Node    Node
	Persist func(Ready) error
	Send    func(Message) error
	Apply   func(Entry) error
}

// Drive persists state, sends messages, applies committed entries, then advances the node.
func (d Driver) Drive() error {
	ready := d.Node.Ready()
	if err := d.Persist(ready); err != nil {
		return err
	}
	for _, message := range ready.Messages {
		if err := d.Send(message); err != nil {
			return err
		}
	}
	for _, entry := range ready.CommittedEntries {
		if err := d.Apply(entry); err != nil {
			return err
		}
	}
	d.Node.Advance()
	return nil
}
