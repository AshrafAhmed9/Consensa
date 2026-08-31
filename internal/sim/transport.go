package sim

import "errors"

// NodeID identifies a node for the lifetime of one cluster.
type NodeID uint64

// Transport is the narrow boundary between a protocol and its network.
type Transport interface {
	Send(to NodeID, msg []byte) error
	Recv() ([]byte, NodeID, error)
}

var errNoMessage = errors.New("sim: no message available")

// ErrNoMessage reports that a non-blocking simulated receive found no delivery.
var ErrNoMessage = errNoMessage

type endpoint struct {
	id        NodeID
	scheduler *Scheduler
	inbox     []envelope
}

// Send queues a copy: callers may safely reuse their buffer after this method returns.
func (e *endpoint) Send(to NodeID, msg []byte) error { return e.scheduler.enqueue(e.id, to, msg) }

// Recv removes the next delivered message without waiting.
func (e *endpoint) Recv() ([]byte, NodeID, error) {
	if len(e.inbox) == 0 {
		return nil, 0, ErrNoMessage
	}
	m := e.inbox[0]
	e.inbox = e.inbox[1:]
	return append([]byte(nil), m.payload...), m.from, nil
}
