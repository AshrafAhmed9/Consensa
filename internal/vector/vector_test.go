package vector

import (
	"math"
	"testing"
)

func TestEncodeRoundTrip(t *testing.T) {
	v := Vector{1, -2.5, 0}
	got, e := Decode(v.Encode())
	if e != nil || len(got) != len(v) {
		t.Fatalf("Decode = %v, %v", got, e)
	}
	for i := range v {
		if got[i] != v[i] {
			t.Fatal("round trip changed component")
		}
	}
}
func TestDistances(t *testing.T) {
	a, b := []float32{1, 2, 3}, []float32{4, 6, 3}
	if got := L2Squared(a, b); got != 25 {
		t.Fatalf("L2Squared=%v", got)
	}
	if got := InnerProduct(a, b); got != 25 {
		t.Fatalf("InnerProduct=%v", got)
	}
	if got := CosineDistance([]float32{0}, []float32{1}); !math.IsNaN(float64(got)) {
		t.Fatal("zero cosine must be NaN")
	}
}

func TestValidateDimensionRejectsNonFiniteComponents(t *testing.T) {
	if err := (Vector{float32(math.NaN())}).ValidateDimension(1); err != ErrNonFinite {
		t.Fatalf("NaN validation = %v", err)
	}
}
func FuzzDistanceAgreement(f *testing.F) {
	f.Add(float32(1), float32(2))
	f.Fuzz(func(t *testing.T, x, y float32) {
		got := L2Squared([]float32{x}, []float32{y})
		want := (x - y) * (x - y)
		if got != want && !math.IsNaN(float64(got)) {
			t.Fatalf("got %v want %v", got, want)
		}
	})
}
