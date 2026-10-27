package storage

import (
	"bytes"
	"math/rand/v2"
)

const maxLevel = 16

// record is one key and its value.
type record struct {
	key, value []byte
}

type skipNode struct {
	key, value []byte
	next       []*skipNode // next[i] is the following node on level i
}

// skiplist is a sorted map held in memory. Each node is linked into a random number of
// levels, so a search can skip ahead on the upper levels (about O(log n) steps). Walking
// level 0 gives all records in key order, which is the order an SSTable needs.
// It is not safe for concurrent use; the DB lock protects it.
type skiplist struct {
	head  *skipNode
	level int
	rng   *rand.Rand
	size  int
}

func newSkiplist() *skiplist {
	return &skiplist{
		head:  &skipNode{next: make([]*skipNode, maxLevel)},
		level: 1,
		rng:   rand.New(rand.NewPCG(1, 1^0x517cc1b727220a95)),
	}
}

// randomLevel picks a node's height: each extra level has a 1 in 4 chance.
func (s *skiplist) randomLevel() int {
	n := 1
	for n < maxLevel && s.rng.Uint64()&3 == 0 {
		n++
	}
	return n
}

// findPrevious returns, for each level, the last node whose key is less than key.
func (s *skiplist) findPrevious(key []byte) []*skipNode {
	update := make([]*skipNode, maxLevel)
	x := s.head
	for l := s.level - 1; l >= 0; l-- {
		for x.next[l] != nil && bytes.Compare(x.next[l].key, key) < 0 {
			x = x.next[l]
		}
		update[l] = x
	}
	return update
}

// get returns the value for key, if present.
func (s *skiplist) get(key []byte) ([]byte, bool) {
	x := s.findPrevious(key)[0].next[0]
	if x != nil && bytes.Equal(x.key, key) {
		return x.value, true
	}
	return nil, false
}

// put sets key to value, replacing any existing value.
func (s *skiplist) put(key, value []byte) {
	update := s.findPrevious(key)
	if x := update[0].next[0]; x != nil && bytes.Equal(x.key, key) {
		x.value = append([]byte(nil), value...)
		return
	}
	lvl := s.randomLevel()
	if lvl > s.level {
		for l := s.level; l < lvl; l++ {
			update[l] = s.head
		}
		s.level = lvl
	}
	n := &skipNode{key: append([]byte(nil), key...), value: append([]byte(nil), value...), next: make([]*skipNode, lvl)}
	for l := 0; l < lvl; l++ {
		n.next[l] = update[l].next[l]
		update[l].next[l] = n
	}
	s.size++
}

// all returns every record in key order.
func (s *skiplist) all() []record {
	out := make([]record, 0, s.size)
	for x := s.head.next[0]; x != nil; x = x.next[0] {
		out = append(out, record{key: x.key, value: x.value})
	}
	return out
}
