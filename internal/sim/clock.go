package sim

import "time"

// Clock is the only source of time available to a simulated node.
type Clock interface {
	Now() time.Time
	Tick()
}

// FakeClock advances only when the scheduler advances it. This prevents wall-clock
// timing from making a failure impossible to reproduce.
type FakeClock struct {
	now  time.Time
	step time.Duration
}

// NewFakeClock creates a logical clock with the supplied initial skew and tick size.
func NewFakeClock(start time.Time, skew, step time.Duration) *FakeClock {
	return &FakeClock{now: start.Add(skew), step: step}
}

// Now returns the current logical time.
func (c *FakeClock) Now() time.Time { return c.now }

// Tick advances logical time by its configured step.
func (c *FakeClock) Tick() { c.now = c.now.Add(c.step) }
