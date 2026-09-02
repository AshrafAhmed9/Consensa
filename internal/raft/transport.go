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

// PeerRegistrar is an optional Transport capability: learning a new peer's address after
// construction, rather than only at ListenTCP/Register time. This is what closes the
// joint-consensus provisioning gap docs/notes/11-joint-consensus.md named as missing --
// Node.ProposeConfChange can already add a NodeID Raft has never heard of to a group's
// voter or learner set, but until that ID's address is registered here, Send to it fails
// with "unknown peer address" even though Raft's own membership state already includes
// it. Both *TCPTransport and *rangeView (multiplex.go) implement it; the in-memory
// Cluster simulator's transport does not need to, which is why this is a separate,
// optional interface rather than part of Transport itself.
type PeerRegistrar interface {
	AddPeer(id NodeID, address string)
}
