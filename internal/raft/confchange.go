package raft

import (
	"bytes"
	"encoding/json"
	"errors"
)

// Membership changes (adding or removing voters) are log entries, so they are replicated
// like any other entry. They take two steps, called joint consensus:
//
//  1. A "joint" entry lists the old and new voters. While it is in effect, every vote and
//     commit needs a majority of the old set AND a majority of the new set.
//  2. Once the joint entry commits, the leader appends a second entry holding only the
//     new voters.
//
// Going through the joint step means there is never a moment when two different groups
// could each elect a leader. specs/membership.tla checks that property.
//
// A node uses the newest config entry in its log as soon as it is appended, whether or
// not it has committed. That is part of the algorithm, not a shortcut.

// confChangePrefix marks a log entry as a config change rather than application data.
// It starts with a zero byte, which application commands (JSON) never do.
var confChangePrefix = []byte("\x00raft/confchange\x00")

// jointLeaderNoop is an empty entry a new leader appends when a joint config is still
// in the log. It lets the new leader commit an entry from its own term, which Raft
// needs before it may commit the older joint entry.
var jointLeaderNoop = []byte("\x00raft/joint-leader-noop/v1\x00")

// confChangeEntry is the content of a config-change log entry.
// Joint=true: Old and New are the two voter sets of the joint step.
// Joint=false: New is the final voter set and Old is unused.
type confChangeEntry struct {
	Old, New []NodeID
	Learners []NodeID
	Joint    bool
}

// marshalConfChange turns a config change into the bytes stored in a log entry.
func marshalConfChange(c confChangeEntry) ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return append(append([]byte(nil), confChangePrefix...), data...), nil
}

// decodeConfChange reads a config change from entry data, and reports false if the
// data is not one.
func decodeConfChange(data []byte) (confChangeEntry, bool) {
	if !bytes.HasPrefix(data, confChangePrefix) {
		return confChangeEntry{}, false
	}
	var c confChangeEntry
	if err := json.Unmarshal(data[len(confChangePrefix):], &c); err != nil {
		return confChangeEntry{}, false
	}
	return c, true
}

// isRaftInternalData reports whether an entry belongs to Raft itself. The host skips
// these instead of passing them to the application.
func isRaftInternalData(data []byte) bool {
	return bytes.HasPrefix(data, confChangePrefix) || bytes.Equal(data, jointLeaderNoop)
}

// idSet turns a list of IDs into a set.
func idSet(ids []NodeID) map[NodeID]bool {
	set := make(map[NodeID]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}

// idList turns a set of IDs into a list.
func idList(set map[NodeID]bool) []NodeID {
	out := make([]NodeID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
}

// membershipFromConfChange works out the voters that a config entry puts into effect.
func membershipFromConfChange(c confChangeEntry) Membership {
	if !c.Joint {
		return Membership{Old: idSet(c.New), New: map[NodeID]bool{}}
	}
	return Membership{Old: idSet(c.Old), New: idSet(c.New), Joint: true}
}

// recomputeMembership sets the node's voters from the newest config entry in its log, or
// from the starting config if the log has none. It rescans the whole log each time
// because that is the simplest way to stay correct when a follower's log is cut back.
func (n *node) recomputeMembership() {
	membership := n.initialMembership
	membershipIndex := uint64(0)
	for _, e := range n.log.entries {
		if c, ok := decodeConfChange(e.Data); ok {
			membership = membershipFromConfChange(c)
			membershipIndex = e.Index
		}
	}
	n.membership = membership
	n.membershipIndex = membershipIndex
}

// isVoter reports whether id may vote right now (old or new voters while joint).
func (n *node) isVoter(id NodeID) bool { return n.membership.Voters()[id] }

// votingPeers lists the other voters. While joint this includes the new voters, since a
// majority of them is needed too.
func (n *node) votingPeers() []NodeID {
	out := make([]NodeID, 0)
	for id := range n.membership.Voters() {
		if id != n.id {
			out = append(out, id)
		}
	}
	return out
}

// AddKnownPeer lets this node know about a new node so it can be named in a config
// change. It does not change who votes and is not replicated.
func (n *node) AddKnownPeer(id NodeID) {
	for _, p := range n.peers {
		if p == id {
			return
		}
	}
	n.peers = append(n.peers, id)
	if n.role == Leader {
		// Without this, the leader would compute a nonsense "previous index" for the new peer.
		n.next[id] = n.log.lastIndex() + 1
	}
}

// ProposeConfChange starts a voter change by appending the joint entry. It fails if this
// node is not the leader, if a change is already running, or if any named node is
// unknown. Moving the cluster to a brand-new machine also needs the transport to learn
// its address (see Host.AddPeer).
func (n *node) ProposeConfChange(newVoters, newLearners []NodeID) error {
	if n.role != Leader {
		return errors.New("raft: confChange proposal to non-leader")
	}
	if n.membership.Joint {
		return errors.New("raft: membership change already in progress")
	}
	if len(newVoters) == 0 {
		return errors.New("raft: empty voter set")
	}
	known := idSet(n.peers)
	for _, id := range append(append([]NodeID(nil), newVoters...), newLearners...) {
		if !known[id] {
			return errors.New("raft: confChange target is not a known peer")
		}
	}
	voters := idSet(newVoters)
	for _, id := range newLearners {
		if voters[id] {
			return errors.New("raft: confChange voter cannot also be a learner")
		}
	}
	data, err := marshalConfChange(confChangeEntry{
		Old:      idList(n.membership.Old),
		New:      append([]NodeID(nil), newVoters...),
		Learners: append([]NodeID(nil), newLearners...),
		Joint:    true,
	})
	if err != nil {
		return err
	}
	return n.proposeInternal(data)
}

// proposeLeaveJoint appends the second entry, which keeps only the new voters. The
// leader calls it by itself once the joint entry has committed.
func (n *node) proposeLeaveJoint() error {
	data, err := marshalConfChange(confChangeEntry{New: idList(n.membership.New), Joint: false})
	if err != nil {
		return err
	}
	return n.proposeInternal(data)
}
