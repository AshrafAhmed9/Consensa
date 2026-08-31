package sim

import (
	"errors"
	"math/rand/v2"
	"sort"
	"time"
)

// Faults controls fault injection. Partitions permit traffic only within the same group;
// nodes omitted from all groups are isolated.
type Faults struct {
	DropRate, DuplicateRate float64
	MaxDelay                time.Duration
	Partitions              [][]NodeID
	ClockSkew               map[NodeID]time.Duration
}
type envelope struct {
	from, to NodeID
	payload  []byte
	due      time.Time
	sequence uint64
}

// Scheduler owns every simulated delivery decision. It is deliberately single-threaded:
// concurrent test code would reintroduce the nondeterminism this package exists to remove.
type Scheduler struct {
	rng       *rand.Rand
	now       time.Time
	step      time.Duration
	faults    Faults
	endpoints map[NodeID]*endpoint
	clocks    map[NodeID]*FakeClock
	pending   []envelope
	sequence  uint64
	schedule  []string
}

// NewScheduler creates a seeded scheduler. Seed is sufficient to replay its choices.
func NewScheduler(seed uint64, start time.Time, step time.Duration, faults Faults) *Scheduler {
	return &Scheduler{rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), now: start, step: step, faults: faults, endpoints: make(map[NodeID]*endpoint), clocks: make(map[NodeID]*FakeClock)}
}

// AddNode attaches a node and returns its transport and logical clock.
func (s *Scheduler) AddNode(id NodeID) (Transport, Clock) {
	e := &endpoint{id: id, scheduler: s}
	s.endpoints[id] = e
	c := NewFakeClock(s.now, s.faults.ClockSkew[id], s.step)
	s.clocks[id] = c
	return e, c
}
func (s *Scheduler) enqueue(from, to NodeID, p []byte) error {
	if s.endpoints[to] == nil {
		return errors.New("sim: destination does not exist")
	}
	if !s.connected(from, to) || s.rng.Float64() < s.faults.DropRate {
		s.schedule = append(s.schedule, "drop")
		return nil
	}
	s.add(from, to, p)
	if s.rng.Float64() < s.faults.DuplicateRate {
		s.add(from, to, p)
	}
	return nil
}
func (s *Scheduler) add(from, to NodeID, p []byte) {
	delay := time.Duration(0)
	if s.faults.MaxDelay > 0 {
		delay = time.Duration(s.rng.Int64N(int64(s.faults.MaxDelay) + 1))
	}
	s.sequence++
	s.pending = append(s.pending, envelope{from: from, to: to, payload: append([]byte(nil), p...), due: s.now.Add(delay), sequence: s.sequence})
}
func (s *Scheduler) connected(a, b NodeID) bool {
	if len(s.faults.Partitions) == 0 {
		return true
	}
	for _, g := range s.faults.Partitions {
		aa, bb := false, false
		for _, id := range g {
			aa = aa || id == a
			bb = bb || id == b
		}
		if aa && bb {
			return true
		}
	}
	return false
}

// Tick advances all clocks once and delivers every message whose delay has elapsed.
func (s *Scheduler) Tick() {
	s.now = s.now.Add(s.step)
	for _, c := range s.clocks {
		c.Tick()
	}
	sort.SliceStable(s.pending, func(i, j int) bool {
		if s.pending[i].due.Equal(s.pending[j].due) {
			return s.pending[i].sequence < s.pending[j].sequence
		}
		return s.pending[i].due.Before(s.pending[j].due)
	})
	n := 0
	for n < len(s.pending) && !s.pending[n].due.After(s.now) {
		m := s.pending[n]
		s.endpoints[m.to].inbox = append(s.endpoints[m.to].inbox, m)
		s.schedule = append(s.schedule, "deliver")
		n++
	}
	s.pending = s.pending[n:]
}

// Schedule returns an immutable copy of delivery decisions for replay assertions.
func (s *Scheduler) Schedule() []string { return append([]string(nil), s.schedule...) }
