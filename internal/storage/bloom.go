package storage

import "hash/fnv"

// bloom is a small deterministic Bloom filter. False positives cost one data-block scan;
// false negatives would lose data, so its bit-setting and querying paths are identical.
type bloom struct {
	bits   []byte
	hashes uint8
}

func newBloom(keys [][]byte) *bloom {
	n := len(keys) * 10
	if n < 64 {
		n = 64
	}
	b := &bloom{bits: make([]byte, (n+7)/8), hashes: 5}
	for _, k := range keys {
		b.add(k)
	}
	return b
}
func (b *bloom) positions(k []byte) []uint64 {
	h := fnv.New64a()
	_, _ = h.Write(k)
	a := h.Sum64()
	return []uint64{a, a >> 11, a >> 22, a >> 33, a >> 44}
}
func (b *bloom) add(k []byte) {
	for _, p := range b.positions(k) {
		i := p % uint64(len(b.bits)*8)
		b.bits[i/8] |= 1 << uint(i%8)
	}
}
func (b *bloom) mayContain(k []byte) bool {
	for _, p := range b.positions(k) {
		i := p % uint64(len(b.bits)*8)
		if b.bits[i/8]&(1<<uint(i%8)) == 0 {
			return false
		}
	}
	return true
}
