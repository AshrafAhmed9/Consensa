package vector

import "math"

// L2Squared returns the squared Euclidean distance between a and b. The square root is
// skipped because it never changes which vector is nearer, and this runs for every
// comparison during a search. If the lengths differ the result is NaN.
func L2Squared(a, b []float32) float32 {
	if len(a) != len(b) {
		return float32(math.NaN())
	}
	// The main loop handles eight components per step, which lets the compiler skip
	// most bounds checks and runs noticeably faster than one component at a time.
	n := len(a) &^ 7 // length rounded down to a multiple of 8
	a8, b8 := a[:n], b[:n]
	var sum float32
	for i := 0; i < n; i += 8 {
		sum += sq(a8[i], b8[i]) + sq(a8[i+1], b8[i+1]) + sq(a8[i+2], b8[i+2]) + sq(a8[i+3], b8[i+3]) +
			sq(a8[i+4], b8[i+4]) + sq(a8[i+5], b8[i+5]) + sq(a8[i+6], b8[i+6]) + sq(a8[i+7], b8[i+7])
	}
	// Handle the last few components, if the length is not a multiple of 8.
	for i := n; i < len(a); i++ {
		sum += sq(a[i], b[i])
	}
	return sum
}

// sq returns the square of the difference x-y.
func sq(x, y float32) float32 {
	d := x - y
	return d * d
}
