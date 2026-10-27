package vector

import (
	"math"
	"testing"
)

func TestL2Squared(t *testing.T) {
	a, b := []float32{1, 2, 3}, []float32{4, 6, 3}
	if got := L2Squared(a, b); got != 25 {
		t.Fatalf("L2Squared=%v", got)
	}
	if got := L2Squared([]float32{1}, []float32{1, 2}); !math.IsNaN(float64(got)) {
		t.Fatal("different lengths must give NaN")
	}
}

// TestL2SquaredLongVectors uses 19 components so both the eight-at-a-time loop and the
// leftover loop run, and checks the result against a plain loop.
func TestL2SquaredLongVectors(t *testing.T) {
	a, b := make([]float32, 19), make([]float32, 19)
	var want float32
	for i := range a {
		a[i], b[i] = float32(i), float32(2*i)
		want += (a[i] - b[i]) * (a[i] - b[i])
	}
	if got := L2Squared(a, b); got != want {
		t.Fatalf("L2Squared=%v want %v", got, want)
	}
}

func TestValidateDimension(t *testing.T) {
	if err := (Vector{float32(math.NaN())}).ValidateDimension(1); err != ErrNonFinite {
		t.Fatalf("NaN validation = %v", err)
	}
	if err := (Vector{1, 2}).ValidateDimension(3); err != ErrDimensionMismatch {
		t.Fatalf("length validation = %v", err)
	}
	if err := (Vector{1, 2}).ValidateDimension(2); err != nil {
		t.Fatalf("valid vector rejected: %v", err)
	}
}
