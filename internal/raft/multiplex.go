package raft

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"
)

// MultiplexedTransport is one real TCP listener shared by every range Host running on
// this node, instead of each range's Host opening its own dedicated socket the way
// TCPTransport does. This is the piece PLAN.md's Phase 3 section names ("all ranges
// share one transport... 1,000 ranges must not mean 1,000x heartbeat traffic"), and it
// now closes both halves of that claim: one socket accepts frames for every range on the
// listening side (each frame carries the range ID identifying which range's Host should
// receive it), and on the sending side every range's Host sharing a destination node
// reuses one persistent outbound TCP connection to it instead of dialing per message --
// see connFor/pooledConn below. What this still does NOT do: coalesce multiple ranges'
// messages into a single frame (each message is still its own length-prefixed frame,
// just sent over a shared, already-open connection instead of a freshly dialed one) --
// that would require batching Host.Ready outputs across ranges at the caller, which
// stays out of scope here the same way it stays out of scope for TCPTransport.
type MultiplexedTransport struct {
	id       NodeID
	listener net.Listener
	mu       sync.Mutex
	views    map[uint64]*rangeView
	conns    map[string]*pooledConn
	done     chan struct{}
	close    sync.Once
}

// pooledConn is one persistent outbound connection to a peer address, shared by every
// range on this node sending to that same destination. writeMu serializes frame writes:
// net.Conn.Write is not safe for concurrent callers, and without this lock two ranges'
// frames sent at the same moment could interleave their bytes on the wire.
type pooledConn struct {
	writeMu sync.Mutex
	conn    net.Conn
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
	m := &MultiplexedTransport{id: id, listener: listener, views: map[uint64]*rangeView{}, conns: map[string]*pooledConn{}, done: make(chan struct{})}
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
	view := newRangeView(m.id, rangeID, peers, m)
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
		m.mu.Lock()
		for _, pc := range m.conns {
			_ = pc.conn.Close()
		}
		m.conns = map[string]*pooledConn{}
		m.mu.Unlock()
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

// receive reads frames from one inbound connection in a loop until it errors or closes,
// rather than one-frame-and-close: this is what makes a sender's persistent pooledConn
// actually save connection setup cost -- a receiver that closed after the first frame
// would force a fresh dial on every subsequent message regardless of what the sending
// side pools.
func (m *MultiplexedTransport) receive(connection net.Conn) {
	defer func() { _ = connection.Close() }()
	buffered := bufio.NewReader(connection)
	for {
		data, err := readFrame(buffered)
		if err != nil {
			return
		}
		var env envelope
		if json.Unmarshal(data, &env) != nil || env.Message.To != m.id {
			continue
		}
		m.mu.Lock()
		view := m.views[env.RangeID]
		m.mu.Unlock()
		if view == nil {
			continue
		}
		view.deliver(env.Message)
	}
}

// rangeView is one range's logical Transport, sharing its node's single real
// MultiplexedTransport listener. It satisfies the same Transport interface *TCPTransport
// does, so Host cannot tell the difference.
//
// inbox exists because inbound connections are now pooled and read in a loop
// (MultiplexedTransport.receive): a single physical connection between two nodes carries
// every range's traffic between them, read by one goroutine. Calling a slow range's
// handler synchronously from that goroutine would delay every OTHER range sharing that
// connection from ever having its own next frame read -- head-of-line blocking that did
// not exist when each range dialed and accepted its own independent connection. Each view
// instead gets its own bounded inbox and worker goroutine: receive() only has to enqueue
// and move on, ranges no longer share a processing goroutine with each other, and this
// range's own messages are still delivered to its handler strictly in the order the
// sender put them on the wire -- Raft's AppendEntries ordering assumption requires that
// per-range order, but never required cross-range ordering to begin with. Found as a real
// test flake (an election that should have been stable under a 3-second deadline
// occasionally never converged) after adding connection pooling, not by inspection.
type rangeView struct {
	id       NodeID
	rangeID  uint64
	peers    map[NodeID]string
	listener *MultiplexedTransport

	mu      sync.Mutex
	handler func(Message) error

	inbox      chan Message
	workerStop chan struct{}
	workerDone sync.Once
}

const rangeViewInboxSize = 256

func newRangeView(id NodeID, rangeID uint64, peers map[NodeID]string, listener *MultiplexedTransport) *rangeView {
	v := &rangeView{
		id: id, rangeID: rangeID, peers: peers, listener: listener,
		inbox: make(chan Message, rangeViewInboxSize), workerStop: make(chan struct{}),
	}
	go v.work()
	return v
}

func (v *rangeView) work() {
	for {
		select {
		case <-v.workerStop:
			return
		case message := <-v.inbox:
			v.mu.Lock()
			handler := v.handler
			v.mu.Unlock()
			if handler != nil {
				_ = handler(message)
			}
		}
	}
}

// deliver enqueues an inbound message for this range's own worker goroutine. A full inbox
// (this range's handler falling far behind the network) drops the message rather than
// blocking the shared connection's read loop -- exactly the message loss Raft is already
// designed to tolerate (a dropped heartbeat or AppendEntries is retried by the protocol
// itself), traded deliberately for never letting one range's backlog stall another's.
func (v *rangeView) deliver(message Message) {
	select {
	case v.inbox <- message:
	default:
	}
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
// of reaching a Host that may already be shutting down, and stops this range's worker
// goroutine. It deliberately does not close the shared listener -- that would sever every
// other range on this node.
func (v *rangeView) Close() error {
	v.mu.Lock()
	v.handler = nil
	v.mu.Unlock()
	v.workerDone.Do(func() { close(v.workerStop) })
	return nil
}

// Send encodes message with this range's ID and writes it over the destination's shared
// pooled connection -- reused by every other range on this node also sending to that same
// peer, instead of TCPTransport.Send's per-message dial.
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
	return v.listener.send(address, data)
}

// send writes data as one frame over the pooled connection to address, dialing lazily on
// first use or after a prior failure. A write error drops the pooled connection (it is
// assumed dead -- the peer may have restarted, or the connection idled out) and the next
// call redials rather than retrying immediately, so one bad message cannot wedge every
// range sharing this destination in a retry loop.
func (m *MultiplexedTransport) send(address string, data []byte) error {
	pc, err := m.connFor(address)
	if err != nil {
		return err
	}
	pc.writeMu.Lock()
	writeErr := writeFrame(pc.conn, data)
	pc.writeMu.Unlock()
	if writeErr != nil {
		m.mu.Lock()
		if m.conns[address] == pc {
			delete(m.conns, address)
		}
		m.mu.Unlock()
		_ = pc.conn.Close()
	}
	return writeErr
}

// connFor returns the pooled connection for address, dialing a fresh one if none exists
// yet. Double-checked under the same lock as the map itself so two ranges racing to send
// to a newly-seen peer at the same moment cannot each open their own connection.
func (m *MultiplexedTransport) connFor(address string) (*pooledConn, error) {
	m.mu.Lock()
	if pc, ok := m.conns[address]; ok {
		m.mu.Unlock()
		return pc, nil
	}
	m.mu.Unlock()

	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		return nil, err
	}
	pc := &pooledConn{conn: conn}

	m.mu.Lock()
	if existing, ok := m.conns[address]; ok {
		m.mu.Unlock()
		_ = conn.Close()
		return existing, nil
	}
	m.conns[address] = pc
	m.mu.Unlock()
	return pc, nil
}
