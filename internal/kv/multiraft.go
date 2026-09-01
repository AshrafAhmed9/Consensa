package kv

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/ashraf/consensa/internal/raft"
)

// MultiRaft multiplexes independently elected range groups through one deterministic
// scheduler. The groups share a tick loop but never share logs or commit indices.
type MultiRaft struct {
	groups      map[uint64]*raft.Cluster
	states      map[uint64]map[raft.NodeID]*rangeState
	appliedNext map[uint64]map[raft.NodeID]int
}

// NewMultiRaft creates no range groups; callers add static descriptors explicitly.
func NewMultiRaft() *MultiRaft {
	return &MultiRaft{
		groups:      map[uint64]*raft.Cluster{},
		states:      map[uint64]map[raft.NodeID]*rangeState{},
		appliedNext: map[uint64]map[raft.NodeID]int{},
	}
}

// AddGroup attaches one range's fixed Raft replica set.
func (m *MultiRaft) AddGroup(rangeID uint64, replicas []raft.NodeID) error {
	if rangeID == 0 {
		return errors.New("kv: zero range ID")
	}
	if m.groups[rangeID] != nil {
		return errors.New("kv: duplicate range group")
	}
	group, err := raft.NewCluster(replicas)
	if err != nil {
		return err
	}
	m.groups[rangeID] = group
	m.states[rangeID] = make(map[raft.NodeID]*rangeState, len(replicas))
	m.appliedNext[rangeID] = make(map[raft.NodeID]int, len(replicas))
	for _, replica := range replicas {
		m.states[rangeID][replica] = newRangeState()
	}
	return nil
}

// RemoveGroup removes a static range after its replacement descriptors and groups are
// ready. Callers publish metadata separately; a stale client receives RangeKeyMismatch
// and refreshes instead of writing to a range that no longer owns its key span.
func (m *MultiRaft) RemoveGroup(rangeID uint64) error {
	if m.groups[rangeID] == nil {
		return errors.New("kv: unknown range group")
	}
	delete(m.groups, rangeID)
	delete(m.states, rangeID)
	delete(m.appliedNext, rangeID)
	return nil
}

// Tick batches one heartbeat/election tick across every hosted range group.
func (m *MultiRaft) Tick() error {
	for _, group := range m.groups {
		if err := group.Tick(); err != nil {
			return err
		}
	}
	return m.applyCommitted()
}

// Leader returns one range group's elected leader for routing and status surfaces.
func (m *MultiRaft) Leader(rangeID uint64) (raft.NodeID, bool) {
	group := m.groups[rangeID]
	if group == nil {
		return 0, false
	}
	return group.Leader()
}

// Propose routes one command to the elected leader of its target range.
func (m *MultiRaft) Propose(rangeID uint64, data []byte) error {
	group := m.groups[rangeID]
	if group == nil {
		return ErrRangeKeyMismatch
	}
	leader, ok := group.Leader()
	if !ok {
		return errors.New("kv: range has no leader")
	}
	if err := group.Propose(leader, data); err != nil {
		return err
	}
	return m.applyCommitted()
}

// Applied exposes a copy of commands applied by one replica of one range for deterministic tests.
func (m *MultiRaft) Applied(rangeID uint64, replica raft.NodeID) [][]byte {
	group := m.groups[rangeID]
	if group == nil {
		return nil
	}
	return group.Applied(replica)
}

// Put replicates one key/value mutation through the key's range group. It is deliberately
// a tiny state-machine command, rather than a direct map update: only Raft-applied commands
// may alter a range, so every replica observes the same order.
func (m *MultiRaft) Put(rangeID uint64, key, value []byte) error {
	if len(key) == 0 {
		return errors.New("kv: empty key")
	}
	data, err := marshalRangeCommand(rangeCommand{Type: commandPut, Key: key, Value: value})
	if err != nil {
		return err
	}
	return m.Propose(rangeID, data)
}

// Delete replicates a removal through the target range group.
func (m *MultiRaft) Delete(rangeID uint64, key []byte) error {
	if len(key) == 0 {
		return errors.New("kv: empty key")
	}
	data, err := marshalRangeCommand(rangeCommand{Type: commandDelete, Key: key})
	if err != nil {
		return err
	}
	return m.Propose(rangeID, data)
}

// Get returns the value visible on one range replica. Callers select a leader until the
// lease/read-index protocol exists; exposing replicas here lets tests prove convergence.
func (m *MultiRaft) Get(rangeID uint64, replica raft.NodeID, key []byte) ([]byte, error) {
	state := m.states[rangeID][replica]
	if state == nil {
		return nil, errors.New("kv: unknown range replica")
	}
	return state.get(key)
}

func (m *MultiRaft) applyCommitted() error {
	for rangeID, group := range m.groups {
		for replica, state := range m.states[rangeID] {
			commands := group.Applied(replica)
			next := m.appliedNext[rangeID][replica]
			for _, data := range commands[next:] {
				if err := state.apply(data); err != nil {
					return err
				}
				next++
			}
			m.appliedNext[rangeID][replica] = next
		}
	}
	return nil
}

type commandType string

const (
	commandPut             commandType = "put"
	commandDelete          commandType = "delete"
	commandLease           commandType = "lease"
	commandClosedTimestamp commandType = "closed_timestamp"
)

// rangeCommand's lease fields carry a Lease grant (see lease.go) through the same
// Raft-replicated wire format as Put/Delete: a lease is only safe to act on once every
// replica agrees the leader actually granted it, which is exactly the ordering guarantee
// Raft already gives ordinary writes. Timestamps are absolute (RFC3339Nano wall-clock
// values assigned once by the proposing leader), not re-derived per replica, so every
// replica applies the identical lease interval regardless of when it individually applies
// the entry.
//
// ClosedTimestamp carries a commandClosedTimestamp command's promised bound the same way:
// fixed once by the proposing leader, replicated, and applied identically everywhere --
// except each replica pairs it with ITS OWN entry.Index at apply time (durable_range.go),
// since "how far this specific replica has applied" is inherently per-replica, unlike the
// timestamp bound itself.
type rangeCommand struct {
	Type            commandType `json:"type"`
	Key             []byte      `json:"key,omitempty"`
	Value           []byte      `json:"value,omitempty"`
	LeaseHolder     raft.NodeID `json:"lease_holder,omitempty"`
	LeaseStart      time.Time   `json:"lease_start,omitempty"`
	LeaseExpiration time.Time   `json:"lease_expiration,omitempty"`
	ClosedTimestamp time.Time   `json:"closed_timestamp,omitempty"`
}

var rangeCommandPrefix = []byte("consensa/kv/v1:")

func marshalRangeCommand(command rangeCommand) ([]byte, error) {
	data, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	return append(bytes.Clone(rangeCommandPrefix), data...), nil
}

// rangeState is intentionally independent from storage.DB. It makes the deterministic
// state-machine boundary explicit first; wiring its apply path to durable MVCC storage
// requires the live node/transport lifecycle that remains outside this in-memory assembly.
type rangeState struct{ values map[string][]byte }

func newRangeState() *rangeState { return &rangeState{values: map[string][]byte{}} }

func (s *rangeState) apply(data []byte) error {
	command, ok, err := decodeRangeCommand(data)
	if err != nil || !ok {
		return err
	}
	switch command.Type {
	case commandPut:
		s.values[string(command.Key)] = bytes.Clone(command.Value)
	case commandDelete:
		delete(s.values, string(command.Key))
	case commandLease, commandClosedTimestamp:
		// This in-memory harness models Put/Delete convergence only; lease and
		// closed-timestamp state are tracked by DurableRange (see durable_range.go),
		// the real replicated implementation these tests don't exercise.
	default:
		return errors.New("kv: unknown range command")
	}
	return nil
}

// decodeRangeCommand recognizes and decodes one range-namespaced Raft entry. ok is false
// (with a nil error) for an entry outside this namespace -- Raft accepts arbitrary
// application entries, and a range host owns only its own namespaced commands, leaving
// room for other command families (vector mutations, future transactional commands) to
// share the same Raft group in a later phase. Shared by rangeState (the in-memory
// MultiRaft state machine) and DurableRange (the real-storage state machine in
// durable_range.go) so both apply byte-identical semantics to the same wire format.
func decodeRangeCommand(data []byte) (rangeCommand, bool, error) {
	if !bytes.HasPrefix(data, rangeCommandPrefix) {
		return rangeCommand{}, false, nil
	}
	var command rangeCommand
	if err := json.Unmarshal(data[len(rangeCommandPrefix):], &command); err != nil {
		return rangeCommand{}, false, err
	}
	// A lease grant or closed-timestamp advance carries no Key -- each authorizes/promises
	// something over the whole range, not one entry -- so the empty-key rejection below
	// only applies to Put/Delete.
	if command.Type != commandLease && command.Type != commandClosedTimestamp && len(command.Key) == 0 {
		return rangeCommand{}, false, errors.New("kv: command has empty key")
	}
	return command, true, nil
}

func (s *rangeState) get(key []byte) ([]byte, error) {
	value, ok := s.values[string(key)]
	if !ok {
		return nil, errors.New("kv: key not found")
	}
	return bytes.Clone(value), nil
}
