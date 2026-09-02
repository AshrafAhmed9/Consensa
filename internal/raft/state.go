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

// ConfState is the Raft membership state captured with a Snapshot. A snapshot discards
// the log entries that established its configuration, so treating membership as only
// log-derived would make a restored replica silently return to its startup configuration.
// Old must be non-empty when a snapshot carries configuration; an empty ConfState denotes
// a legacy snapshot and leaves the configured initial membership in effect.
type ConfState struct {
	Old, New []NodeID
	Joint    bool
}

// Snapshot replaces all log entries through Index with application state and the
// membership state effective at that index.
type Snapshot struct {
	Index, Term uint64
	Data        []byte
	ConfState   ConfState
}

// MessageType enumerates Raft RPCs plus local proposal messages.
type MessageType uint8

// MsgPreVote through MsgTimeoutNow are MessageType's values: the pre-vote and real-vote
// request/response pairs, log replication and its response, snapshot installation, and a
// leader-initiated transfer nudge.
const (
	MsgPreVote MessageType = iota
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgAppend
	MsgAppendResp
	MsgSnapshot
	// MsgTimeoutNow is sent only by a leader that already holds this term's leadership and
	// has confirmed the recipient's log is fully caught up (see node.TransferLeadershipTo).
	// Unlike a normal election timeout, the recipient skips pre-vote and calls
	// startElection directly: the sender, being current leader, has already done the
	// safety check pre-vote exists to do (would this election actually be viable), so
	// repeating it here would only add a needless round trip to what should be a fast
	// handoff.
	MsgTimeoutNow
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

// Config defines a Raft group's initial peer universe. Election ticks should be several
// heartbeats to avoid stable leaders being replaced by ordinary heartbeat jitter.
type Config struct {
	ID    NodeID
	Peers []NodeID
	// Learners are members of Peers that receive log replication (so they can catch up
	// before being promoted to a full voter) but never vote and are never counted toward
	// quorum -- Raft's own answer to the problem PLAN.md's Phase 11 section names: adding
	// a brand-new, empty replica directly as a voter temporarily WEAKENS fault tolerance
	// (the cluster now needs a majority that includes a replica that has nothing yet),
	// where adding it as a learner first does not, since quorum() never counts it. Every
	// ID in Learners must also appear in Peers. A live reconfiguration may promote an
	// existing learner or change voters, but cannot add an ID outside this transport-known
	// peer universe.
	Learners                    []NodeID
	ElectionTick, HeartbeatTick int
}
