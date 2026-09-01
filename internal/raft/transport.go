package raft

import "net"

// Transport is what Host needs from its network adapter: address, best-effort send, and
// shutdown. *TCPTransport (one listener per Host, the default) and *rangeView (one of
// potentially many logical views sharing one *MultiplexedTransport's single real
// listener) both satisfy it, so Host itself never needs to know which one it has.
type Transport interface {
	Addr() net.Addr
	Send(Message) error
	Close() error
}
