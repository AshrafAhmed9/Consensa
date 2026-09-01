package raft

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"
)

// MultiplexedTransport is one real TCP listener shared by every range Host running on
// this node, instead of each range's Host opening its own dedicated socket the way
// TCPTransport does. This is the piece PLAN.md's Phase 3 section names and defers ("all
// ranges share one transport... 1,000 ranges must not mean 1,000x heartbeat traffic") --
// stated honestly, this closes only the "one shared listener" half of that claim.
// Heartbeat batching/coalescing across ranges sharing a destination node is a real,
// separate optimization this type does not attempt: every range's Host still calls Send
// independently, and each Send here still dials its own outbound TCP connection per
// message, exactly like TCPTransport.Send. What changes is only the listening side: one
// socket accepts frames for every range instead of one socket per range, and each frame
// carries the range ID that identifies which range's Host should receive it.
type MultiplexedTransport struct {
	id       NodeID
	listener net.Listener
	mu       sync.Mutex
	views    map[uint64]*rangeView
	done     chan struct{}
	close    sync.Once
}

// envelope wraps a wire-independent Message with the range ID that identifies its
// destination Host on the receiving node. Message itself deliberately carries no such
// field -- see transport.go's doc comment: the pure Raft algorithm (Node, Cluster) has no
// concept of "range" at all, and adding one to Message would leak a transport-layer,
// multi-range concern into code whose whole value is not knowing about either.
type envelope struct {
	RangeID uint64
	Message Message
}

// ListenMultiplexed starts the one shared listener. Individual ranges attach afterward via
// Register; a frame for a range ID nothing has registered yet is silently dropped, the
// same way TCPTransport silently drops a frame addressed to the wrong node ID.
func ListenMultiplexed(id NodeID, address string) (*MultiplexedTransport, error) {
	if id == 0 {
		return nil, errors.New("raft: transport ID required")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	m := &MultiplexedTransport{id: id, listener: listener, views: map[uint64]*rangeView{}, done: make(chan struct{})}
	go m.accept()
	return m, nil
}

// Addr returns the one bound listener address every range on this node shares.
func (m *MultiplexedTransport) Addr() net.Addr { return m.listener.Addr() }

// Register creates this node's view of one range, to be passed as HostConfig.Transport.
// The returned view can send immediately; it will not deliver inbound messages until
// SetHandler is called (normally right after the corresponding Host is constructed, since
// the handler is that Host's own Step method -- see multiplex_test.go for the wiring this
// implies).
func (m *MultiplexedTransport) Register(rangeID uint64, peers map[NodeID]string) *rangeView {
	view := &rangeView{id: m.id, rangeID: rangeID, peers: peers, listener: m}
	m.mu.Lock()
	m.views[rangeID] = view
	m.mu.Unlock()
	return view
}

// Close stops accepting new connections for every range sharing this transport at once --
// there is no per-range detach, matching the "static ranges" scope this session's split
// work (docs/notes/12-split-repair.md) already states plainly for the rest of dynamic
// range lifecycle management.
func (m *MultiplexedTransport) Close() error {
	var err error
	m.close.Do(func() {
		close(m.done)
		err = m.listener.Close()
	})
	return err
}

func (m *MultiplexedTransport) accept() {
	for {
		connection, err := m.listener.Accept()
		if err != nil {
			select {
			case <-m.done:
				return
			default:
				continue
			}
		}
		go m.receive(connection)
	}
}

func (m *MultiplexedTransport) receive(connection net.Conn) {
	defer func() { _ = connection.Close() }()
	data, err := readFrame(connection)
	if err != nil {
		return
	}
	var env envelope
	if json.Unmarshal(data, &env) != nil || env.Message.To != m.id {
		return
	}
	m.mu.Lock()
	view := m.views[env.RangeID]
	m.mu.Unlock()
	if view == nil {
		return
	}
	view.mu.Lock()
	handler := view.handler
	view.mu.Unlock()
	if handler == nil {
		return
	}
	_ = handler(env.Message)
}

// rangeView is one range's logical Transport, sharing its node's single real
// MultiplexedTransport listener. It satisfies the same Transport interface *TCPTransport
// does, so Host cannot tell the difference.
type rangeView struct {
	id       NodeID
	rangeID  uint64
	peers    map[NodeID]string
	listener *MultiplexedTransport

	mu      sync.Mutex
	handler func(Message) error
}

// SetHandler attaches the range's Host.Step after the Host exists -- Register must return
// a usable Transport before the Host it will be passed to has been constructed, so this
// two-step handshake (Register, then NewHost, then SetHandler) is unavoidable rather than
// an oversight; multiplex_test.go documents the exact sequence.
func (v *rangeView) SetHandler(handler func(Message) error) {
	v.mu.Lock()
	v.handler = handler
	v.mu.Unlock()
}

// Addr returns the shared listener's address -- identical for every range on this node.
func (v *rangeView) Addr() net.Addr { return v.listener.Addr() }

// Close detaches this range's handler so late-arriving frames for it are dropped instead
// of reaching a Host that may already be shutting down. It deliberately does not close
// the shared listener -- that would sever every other range on this node.
func (v *rangeView) Close() error {
	v.mu.Lock()
	v.handler = nil
	v.mu.Unlock()
	return nil
}

// Send encodes message with this range's ID and dials the destination directly, the same
// per-message dial TCPTransport.Send uses -- connection pooling/reuse across ranges
// sharing a destination node is exactly the kind of coalescing this type's own doc
// comment says it does not attempt.
func (v *rangeView) Send(message Message) error {
	address, ok := v.peers[message.To]
	if !ok {
		return errors.New("raft: unknown peer address")
	}
	data, err := json.Marshal(envelope{RangeID: v.rangeID, Message: message})
	if err != nil {
		return err
	}
	if len(data) > maxMessageBytes {
		return errors.New("raft: message exceeds transport limit")
	}
	connection, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	return writeFrame(connection, data)
}
