package sim

import (
	"reflect"
	"testing"
	"time"
)

// TestSeedProducesByteIdenticalSchedule proves the test foundation: the same seed must
// describe the same execution, otherwise a chaos failure cannot become a regression test.
func TestSeedProducesByteIdenticalSchedule(t *testing.T) {
	var want []string
	for i := 0; i < 100; i++ {
		s := NewScheduler(7, time.Unix(0, 0), time.Millisecond, Faults{MaxDelay: 2 * time.Millisecond, DuplicateRate: .4})
		a, _ := s.AddNode(1)
		b, _ := s.AddNode(2)
		for n := 0; n < 20; n++ {
			if err := a.Send(2, []byte{byte(n)}); err != nil {
				t.Fatal(err)
			}
			s.Tick()
			_, _, _ = b.Recv()
		}
		got := s.Schedule()
		if i == 0 {
			want = got
		} else if !reflect.DeepEqual(want, got) {
			t.Fatalf("run %d diverged", i)
		}
	}
}
func TestPartitionIsolatesNodes(t *testing.T) {
	s := NewScheduler(1, time.Unix(0, 0), time.Millisecond, Faults{Partitions: [][]NodeID{{1}, {2}}})
	a, _ := s.AddNode(1)
	b, _ := s.AddNode(2)
	_ = a.Send(2, []byte("x"))
	s.Tick()
	if _, _, err := b.Recv(); err != ErrNoMessage {
		t.Fatalf("receive error = %v", err)
	}
}
func TestDelayBound(t *testing.T) {
	s := NewScheduler(1, time.Unix(0, 0), time.Millisecond, Faults{MaxDelay: 3 * time.Millisecond})
	a, _ := s.AddNode(1)
	b, _ := s.AddNode(2)
	_ = a.Send(2, []byte("x"))
	for i := 0; i < 4; i++ {
		s.Tick()
	}
	if _, _, err := b.Recv(); err != nil {
		t.Fatal(err)
	}
}
