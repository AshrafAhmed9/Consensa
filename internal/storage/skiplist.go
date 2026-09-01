package storage

import (
	"bytes"
	"math/rand/v2"
)

const maxLevel = 16

type skipNode struct {
	key, value []byte
	tombstone  bool
	next       []*skipNode
}

// skiplist is a single-writer ordered map. Random tower heights give expected O(log n)
// search while retaining straightforward ordered scans for flushing.
type skiplist struct {
	head  *skipNode
	level int
	rng   *rand.Rand
	size  int
}

func newSkiplist(seed uint64) *skiplist {
	return &skiplist{head: &skipNode{next: make([]*skipNode, maxLevel)}, level: 1, rng: rand.New(rand.NewPCG(seed, seed^0x517cc1b727220a95))}
}
func (s *skiplist) randomLevel() int {
	n := 1
	for n < maxLevel && s.rng.Uint64()&3 == 0 {
		n++
	}
	return n
}
func (s *skiplist) put(key, value []byte, tombstone bool) {
	update := make([]*skipNode, maxLevel)
	x := s.head
	for l := s.level - 1; l >= 0; l-- {
		for x.next[l] != nil && bytes.Compare(x.next[l].key, key) < 0 {
			x = x.next[l]
		}
		update[l] = x
	}
	x = x.next[0]
	if x != nil && bytes.Equal(x.key, key) {
		x.value = append(x.value[:0], value...)
		x.tombstone = tombstone
		return
	}
	lvl := s.randomLevel()
	if lvl > s.level {
		for l := s.level; l < lvl; l++ {
			update[l] = s.head
		}
		s.level = lvl
	}
	n := &skipNode{key: append([]byte(nil), key...), value: append([]byte(nil), value...), tombstone: tombstone, next: make([]*skipNode, lvl)}
	for l := 0; l < lvl; l++ {
		n.next[l] = update[l].next[l]
		update[l].next[l] = n
	}
	s.size++
}
func (s *skiplist) all() []record {
	out := make([]record, 0, s.size)
	for x := s.head.next[0]; x != nil; x = x.next[0] {
		out = append(out, record{key: x.key, value: x.value, tombstone: x.tombstone})
	}
	return out
}
