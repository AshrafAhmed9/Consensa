package txn

import "time"

// Timestamp is a causality-preserving hybrid logical clock value.
type Timestamp struct {
	WallTime int64
	Logical  int32
}

// Compare orders timestamps lexicographically.
func (t Timestamp) Compare(o Timestamp) int {
	if t.WallTime < o.WallTime {
		return -1
	}
	if t.WallTime > o.WallTime {
		return 1
	}
	if t.Logical < o.Logical {
		return -1
	}
	if t.Logical > o.Logical {
		return 1
	}
	return 0
}

// Clock combines physical time with observed timestamps. Callers serialize it; distributed
// causality depends on each received timestamp being observed before a response is emitted.
type Clock struct {
	now  func() time.Time
	last Timestamp
}

// NewClock builds an HLC around an injectable wall-clock source for skew tests.
func NewClock(now func() time.Time) *Clock { return &Clock{now: now} }

// Now returns a monotonic local timestamp even if the physical clock moves backward.
func (c *Clock) Now() Timestamp {
	physical := c.now().UnixNano()
	if physical > c.last.WallTime {
		c.last = Timestamp{WallTime: physical}
	} else {
		c.last.Logical++
	}
	return c.last
}

// Observe incorporates a remote timestamp before issuing a causally later local event.
func (c *Clock) Observe(remote Timestamp) Timestamp {
	physical := c.now().UnixNano()
	max := c.last.WallTime
	if remote.WallTime > max {
		max = remote.WallTime
	}
	if physical > max {
		max = physical
	}
	logical := int32(0)
	if max == c.last.WallTime {
		logical = c.last.Logical + 1
	}
	if max == remote.WallTime && remote.Logical >= logical {
		logical = remote.Logical + 1
	}
	c.last = Timestamp{WallTime: max, Logical: logical}
	return c.last
}
