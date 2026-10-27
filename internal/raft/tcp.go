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

// TCPTransport sends and receives Raft messages over TCP. Each message is one JSON frame
// with a 4-byte length in front, sent on its own short-lived connection.
type TCPTransport struct {
	id       NodeID
	listener net.Listener
	peersMu  sync.RWMutex
	peers    map[NodeID]string
	handle   func(Message) error
	done     chan struct{}
	close    sync.Once
}

// ListenTCP starts listening for messages addressed to id and passes each one to handle.
// peers maps every other node to the address Send should dial.
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

// Addr returns the address being listened on (useful when the port was chosen by the OS).
func (t *TCPTransport) Addr() net.Addr { return t.listener.Addr() }

// AddPeer registers or updates the address of a peer. It is safe to call while sending.
func (t *TCPTransport) AddPeer(id NodeID, address string) {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	t.peers[id] = address
}

// Send delivers one message to its destination, or returns an error if the peer is
// unknown or cannot be reached.
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
	// Without a deadline a stuck peer could block this write forever. The caller holds the
	// host lock while sending, so that would freeze the whole node.
	if err := connection.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	if err := writeFrame(connection, data); err != nil {
		return err
	}
	return nil
}

// Close stops accepting new connections.
func (t *TCPTransport) Close() error {
	var err error
	t.close.Do(func() {
		close(t.done)
		err = t.listener.Close()
	})
	return err
}

// accept takes incoming connections and handles each in its own goroutine.
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

// receive reads one message from a connection and passes it to the handler.
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

// writeFrame writes a 4-byte length followed by the data.
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

// readFrame reads one length-prefixed frame, rejecting sizes above the limit so a bad
// length cannot make us allocate huge memory.
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
