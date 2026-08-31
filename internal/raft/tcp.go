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

// Send encodes one complete message frame and waits for the peer to receive it. The frame
// limit prevents a malformed length prefix from allocating unbounded memory on a node.
func (t *TCPTransport) Send(message Message) error {
	address, ok := t.peers[message.To]
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
	defer connection.Close()
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
	defer connection.Close()
	data, err := readFrame(connection)
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

func readFrame(reader io.Reader) ([]byte, error) {
	buffered := bufio.NewReader(reader)
	var length [4]byte
	if _, err := io.ReadFull(buffered, length[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(length[:])
	if size > maxMessageBytes {
		return nil, errors.New("raft: message exceeds transport limit")
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(buffered, data); err != nil {
		return nil, err
	}
	return data, nil
}
