package kv

import (
	"bytes"
	"errors"
	"github.com/ashraf/consensa/internal/raft"
)

// Descriptor assigns a half-open key span to a fixed replica set.
type Descriptor struct {
	ID         uint64
	Start, End []byte
	Replicas   []raft.NodeID
}

// Contains reports whether key belongs to [Start, End). An empty End is positive infinity.
func (d Descriptor) Contains(key []byte) bool {
	return bytes.Compare(key, d.Start) >= 0 && (len(d.End) == 0 || bytes.Compare(key, d.End) < 0)
}

// ErrRangeKeyMismatch asks the client/router to refresh stale metadata.
var ErrRangeKeyMismatch = errors.New("kv: range key mismatch")
