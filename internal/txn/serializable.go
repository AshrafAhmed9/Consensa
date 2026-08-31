package txn

import "sync"

// TimestampCache records the newest read timestamp for each key. A later write below that
// timestamp is pushed forward, breaking the read-write cycles that permit snapshot-isolation
// write skew.
type TimestampCache struct {
	mu    sync.Mutex
	reads map[string]Timestamp
}

// NewTimestampCache creates an empty per-range read cache.
func NewTimestampCache() *TimestampCache { return &TimestampCache{reads: map[string]Timestamp{}} }

// RecordRead advances the key's high-water mark.
func (c *TimestampCache) RecordRead(key []byte, ts Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if prior, ok := c.reads[string(key)]; !ok || prior.Compare(ts) < 0 {
		c.reads[string(key)] = ts
	}
}

// PushWrite returns the earliest safe write timestamp. A logical increment preserves order
// when a write collides exactly with a recorded read's physical component.
func (c *TimestampCache) PushWrite(key []byte, ts Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	prior, ok := c.reads[string(key)]
	if !ok || prior.Compare(ts) < 0 {
		return ts
	}
	return Timestamp{WallTime: prior.WallTime, Logical: prior.Logical + 1}
}
