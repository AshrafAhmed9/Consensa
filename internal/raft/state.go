package raft

// NodeID identifies a voter in a Raft group.
type NodeID uint64

// Role is the replica's election state.
type Role uint8

// Follower, Candidate, and Leader are Role's three possible values, in the order a
// replica normally passes through them on its way to (or back down from) leadership.
const (
	Follower Role = iota
	Candidate
	Leader
)

// Entry is one replicated state-machine command. Index zero is a permanent sentinel.
type Entry struct {
	Index, Term uint64
	Data        []byte
}

// HardState is the crash-durable portion of a node. It must reach disk before a message
// caused by it reaches the network, otherwise a reboot can contradict an acknowledged vote.
type HardState struct{ Term, Vote, Commit uint64 }

// Snapshot replaces all log entries through Index with application state.
type Snapshot struct {
	Index, Term uint64
	Data        []byte
}

// MessageType enumerates Raft RPCs plus local proposal messages.
type MessageType uint8

// MsgPreVote through MsgSnapshot are MessageType's values: the pre-vote and real-vote
// request/response pairs, log replication and its response, and snapshot installation.
const (
	MsgPreVote MessageType = iota
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgAppend
	MsgAppendResp
	MsgSnapshot
)

// Message is a wire-independent Raft RPC. Its slice fields are copied on receipt so the
// caller may reuse buffers immediately.
type Message struct {
	Type                         MessageType
	From, To                     NodeID
	Term, Index, LogTerm, Commit uint64
	Reject                       bool
	Entries                      []Entry
	Snapshot                     Snapshot
}

// Config defines one fixed Raft group. Election ticks should be several heartbeats to
// avoid stable leaders being replaced by ordinary heartbeat jitter.
type Config struct {
	ID                          NodeID
	Peers                       []NodeID
	ElectionTick, HeartbeatTick int
}
