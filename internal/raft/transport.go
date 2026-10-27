package raft

import "net"

// Transport is what Host needs from its network adapter: address, best-effort send, and
// shutdown. *TCPTransport (one listener per Host, the default) satisfies it, and tests can
// supply their own.
type Transport interface {
	Addr() net.Addr
	Send(Message) error
	Close() error
}

// PeerRegistrar is an optional Transport capability: learning a new peer's address after
// construction, rather than only at ListenTCP/Register time. This is what lets a
// new node be provisioned: Node.ProposeConfChange can already add a NodeID Raft has never
// heard of to a group's voter or learner set, but until that ID's address is registered here, Send to it fails
// with "unknown peer address" even though Raft's own membership state already includes
// it. *TCPTransport implements it; the in-memory
// Cluster simulator's transport does not need to, which is why this is a separate,
// optional interface rather than part of Transport itself.
type PeerRegistrar interface {
	AddPeer(id NodeID, address string)
}
