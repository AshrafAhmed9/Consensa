package raft

// NodeID identifies a node in a Raft group.
type NodeID uint64

// Role is what a node is currently doing in the group.
type Role uint8

// A node starts as a Follower, becomes a Candidate when it asks for votes, and becomes
// Leader if it wins.
const (
	Follower Role = iota
	Candidate
	Leader
)

// Entry is one item in the replicated log. Index 0 is a placeholder.
type Entry struct {
	Index, Term uint64
	Data        []byte
}

// HardState is the part of a node's state that must be saved to disk before it sends any
// message that depends on it. Otherwise a restart could contradict a vote it already gave.
type HardState struct{ Term, Vote, Commit uint64 }

// MessageType is the kind of message nodes send each other.
type MessageType uint8

// The pre-vote and vote requests and replies, and log replication and its reply.
const (
	MsgPreVote MessageType = iota
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgAppend
	MsgAppendResp
)

// Message is one message between nodes. It does not depend on the network used to send it.
type Message struct {
	Type                         MessageType
	From, To                     NodeID
	Term, Index, LogTerm, Commit uint64
	Reject                       bool
	Entries                      []Entry
}

// Config is the starting setup of a node. The election timeout should be several
// heartbeats, so normal heartbeat jitter does not trigger elections.
type Config struct {
	ID    NodeID
	Peers []NodeID
	// Learners are nodes in Peers that receive the log but do not vote. A new, empty node
	// joins as a learner first so it can catch up without weakening fault tolerance, then
	// is promoted to voter. Every learner must also be listed in Peers.
	Learners                    []NodeID
	ElectionTick, HeartbeatTick int
}
