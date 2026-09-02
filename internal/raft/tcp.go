package raft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

const maxMessageBytes = 16 << 20

// TCPTransport is the production-facing message adapter for a Raft host. It uses one
// length-prefixed JSON frame per connection so framing remains visible and testable; Raft
// itself remains a pure state machine that knows nothing about sockets or goroutines.
type TCPTransport struct {
	id       NodeID
	listener net.Listener
	peersMu  sync.RWMutex
	peers    map[NodeID]string
	handle   func(Message) error
	done     chan struct{}
	close    sync.Once
}

// ListenTCP accepts Raft messages for id. peers must contain the remote node addresses
// used by Send; inbound messages are checked before the supplied state-machine callback.
func ListenTCP(id NodeID, address string, peers map[NodeID]string, handle func(Message) error) (*TCPTransport, error) {
	if id == 0 || handle == nil {
		return nil, errors.New("raft: transport ID and handler required")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	transport := &TCPTransport{id: id, listener: listener, peers: make(map[NodeID]string, len(peers)), handle: handle, done: make(chan struct{})}
	for peer, peerAddress := range peers {
		transport.peers[peer] = peerAddress
	}
	go transport.accept()
	return transport, nil
}

// Addr returns the bound listener address, including an OS-assigned port when requested.
func (t *TCPTransport) Addr() net.Addr { return t.listener.Addr() }

// AddPeer implements PeerRegistrar (transport.go) -- see that interface's doc comment.
// Safe to call concurrently with Send: peersMu guards every access to peers.
func (t *TCPTransport) AddPeer(id NodeID, address string) {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	t.peers[id] = address
}

// Send encodes one complete message frame and waits for the peer to receive it. The frame
// limit prevents a malformed length prefix from allocating unbounded memory on a node.
func (t *TCPTransport) Send(message Message) error {
	t.peersMu.RLock()
	address, ok := t.peers[message.To]
	t.peersMu.RUnlock()
	if !ok {
		return errors.New("raft: unknown peer address")
	}
	data, err := json.Marshal(message)
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
	// Without a write deadline, a stalled peer (one whose own receive loop isn't draining
	// its socket -- e.g. because it's itself blocked acquiring its Host's mutex under heavy
	// load) can block this write indefinitely once the kernel's send buffer fills. Host.
	// driveLocked calls Send while holding h.mu (host.go), so an indefinite block here
	// holds that lock forever too -- which can cascade into a real cross-process deadlock
	// if enough peers end up in this state simultaneously (found chasing an apparent hang
	// in a heavy multi-group ann split test under real machine contention: many goroutines
	// stuck inside this exact call, past their supposed 1-second dial timeout, which only
	// bounds DialTimeout itself, not the write that follows). One second matches the dial
	// timeout above; a write that slow indicates the same kind of unhealthy peer dialing
	// already treats as a failure to route around.
	if err := connection.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	if err := writeFrame(connection, data); err != nil {
		return err
	}
	return nil
}

// Close stops accepting new connections. Existing handlers finish their current frame.
func (t *TCPTransport) Close() error {
	var err error
	t.close.Do(func() {
		close(t.done)
		err = t.listener.Close()
	})
	return err
}

func (t *TCPTransport) accept() {
	for {
		connection, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.done:
				return
			default:
				continue
			}
		}
		go t.receive(connection)
	}
}

func (t *TCPTransport) receive(connection net.Conn) {
	defer func() { _ = connection.Close() }()
	data, err := readFrame(bufio.NewReader(connection))
	if err != nil {
		return
	}
	var message Message
	if json.Unmarshal(data, &message) != nil || message.To != t.id {
		return
	}
	_ = t.handle(message)
}

func writeFrame(writer io.Writer, data []byte) error {
	if len(data) > maxMessageBytes {
		return errors.New("raft: message exceeds transport limit")
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(data)))
	if _, err := writer.Write(length[:]); err != nil {
		return err
	}
	_, err := writer.Write(data)
	return err
}

// readFrame reads one length-prefixed frame from reader. The caller owns reader's
// buffering and lifetime -- readFrame used to wrap its argument in a fresh bufio.Reader on
// every call, which is correct for a connection read exactly once (TCPTransport's
// original one-frame-per-connection design) but silently drops data on a connection read
// in a loop: a fresh bufio.Reader's first Read can pull more bytes off the socket than
// this one frame needs, and discarding that bufio.Reader at return time throws away
// whatever of the *next* frame it already buffered. MultiplexedTransport's connection
// pooling (multiplex.go) reads many frames per connection, which is what surfaced this --
// found as an actual test failure (9 of 10 messages missing), not by inspection.
func readFrame(reader *bufio.Reader) ([]byte, error) {
	var length [4]byte
	if _, err := io.ReadFull(reader, length[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(length[:])
	if size > maxMessageBytes {
		return nil, errors.New("raft: message exceeds transport limit")
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}
	return data, nil
}
