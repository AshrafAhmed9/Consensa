package vector

import "math"

// L2Squared returns squared Euclidean distance. Skipping sqrt cannot alter nearest-neighbour
// ordering and keeps the hottest future search operation as cheap as the scalar baseline allows.
// Precondition: a and b have equal lengths.
func L2Squared(a, b []float32) float32 {
	return accumulate(a, b, func(x, y float32) float32 { d := x - y; return d * d })
}

// InnerProduct returns the dot product. Precondition: a and b have equal lengths.
func InnerProduct(a, b []float32) float32 {
	return accumulate(a, b, func(x, y float32) float32 { return x * y })
}

// CosineDistance returns 1-cosine similarity. Zero vectors have no direction, so this returns
// NaN rather than pretending they are near or far; callers must reject or handle them explicitly.
// Precondition: a and b have equal lengths.
func CosineDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		return float32(math.NaN())
	}
	var dot, aa, bb float32
	for i := range a {
		dot += a[i] * b[i]
		aa += a[i] * a[i]
		bb += b[i] * b[i]
	}
	if aa == 0 || bb == 0 {
		return float32(math.NaN())
	}
	return 1 - dot/float32(math.Sqrt(float64(aa*bb)))
}

func accumulate(a, b []float32, f func(float32, float32) float32) float32 {
	if len(a) != len(b) {
		return float32(math.NaN())
	}
	// Reslicing states the loop's eight-element invariant to the compiler, letting it
	// eliminate repeated bounds checks without hiding the scalar algorithm in assembly.
	n := len(a) &^ 7
	a8, b8 := a[:n], b[:n]
	var sum float32
	for i := 0; i < n; i += 8 {
		sum += f(a8[i], b8[i]) + f(a8[i+1], b8[i+1]) + f(a8[i+2], b8[i+2]) + f(a8[i+3], b8[i+3]) + f(a8[i+4], b8[i+4]) + f(a8[i+5], b8[i+5]) + f(a8[i+6], b8[i+6]) + f(a8[i+7], b8[i+7])
	}
	for i := n; i < len(a); i++ {
		sum += f(a[i], b[i])
	}
	return sum
}
