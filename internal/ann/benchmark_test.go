package ann

import (
	"fmt"
	"testing"

	"github.com/ashraf/consensa/internal/vector"
)

// BenchmarkHNSWSearch128 measures deterministic graph traversal on a fixed synthetic corpus.
// It is a baseline, not a claim about real embedding recall or production throughput.
func BenchmarkHNSWSearch128(b *testing.B) {
	index, err := NewHNSW(Config{Dimension: 128, M: 16, EFConstruction: 64, EFSearch: 64, Seed: 1})
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < 256; n++ {
		v := make(vector.Vector, 128)
		for i := range v {
			v[i] = float32((n+i)%37) / 37
		}
		if err := index.Insert(fmt.Sprintf("v-%03d", n), v); err != nil {
			b.Fatal(err)
		}
	}
	query := make(vector.Vector, 128)
	for i := range query {
		query[i] = .5
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if _, err := index.Search(query, 10, 64); err != nil {
			b.Fatal(err)
		}
	}
}
