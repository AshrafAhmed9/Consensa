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
	ID    NodeID
	Peers []NodeID
	// Learners are members of Peers that receive log replication (so they can catch up
	// before being promoted to a full voter) but never vote and are never counted toward
	// quorum -- Raft's own answer to the problem PLAN.md's Phase 11 section names: adding
	// a brand-new, empty replica directly as a voter temporarily WEAKENS fault tolerance
	// (the cluster now needs a majority that includes a replica that has nothing yet),
	// where adding it as a learner first does not, since quorum() never counts it. Every
	// ID in Learners must also appear in Peers; promoting a learner to a voter is done by
	// constructing a new Config without it in Learners, not by a live reconfiguration
	// command -- that step (and the disjoint-majority-safe joint-consensus transition a
	// live add/remove voter change needs) is Membership's (membership.go) job, and it is
	// deliberately still not wired into this quorum path; see docs/adr/010-learners.md.
	Learners                    []NodeID
	ElectionTick, HeartbeatTick int
}
