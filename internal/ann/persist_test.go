package ann

import (
	"bytes"
	"github.com/ashraf/consensa/internal/vector"
	"testing"
)

// TestSnapshotIsCanonical proves independently rebuilt replicas serialize identical graphs.
func TestSnapshotIsCanonical(t *testing.T) {
	a, _ := NewHNSW(Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 9})
	b, _ := NewHNSW(Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 9})
	for _, id := range []string{"a", "b"} {
		v := vector.Vector{float32(len(id)), 0}
		m, e := EncodeMutation(id, v)
		if e != nil {
			t.Fatal(e)
		}
		if e = a.ApplyMutation(m); e != nil {
			t.Fatal(e)
		}
		if e = b.ApplyMutation(m); e != nil {
			t.Fatal(e)
		}
	}
	x, _ := a.Snapshot()
	y, _ := b.Snapshot()
	if !bytes.Equal(x, y) {
		t.Fatal("replica snapshots diverged")
	}
	restored, _ := NewHNSW(Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 9})
	if e := restored.Restore(x); e != nil {
		t.Fatal(e)
	}
	z, _ := restored.Snapshot()
	if !bytes.Equal(x, z) {
		t.Fatal("snapshot restore changed graph")
	}
}
