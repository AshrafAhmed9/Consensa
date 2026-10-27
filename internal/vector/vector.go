package vector

import (
	"errors"
	"math"
)

// Vector is an embedding: a list of float32 numbers.
type Vector []float32

// ErrDimensionMismatch means a vector's length is not the length the index expects.
var ErrDimensionMismatch = errors.New("vector: dimension mismatch")

// ErrNonFinite means a vector contains NaN or infinity, which would make distance
// comparisons meaningless.
var ErrNonFinite = errors.New("vector: non-finite component")

// ValidateDimension checks that v has exactly dimension components and that none is
// NaN or infinite.
func (v Vector) ValidateDimension(dimension int) error {
	if len(v) != dimension {
		return ErrDimensionMismatch
	}
	for _, component := range v {
		if math.IsNaN(float64(component)) || math.IsInf(float64(component), 0) {
			return ErrNonFinite
		}
	}
	return nil
}
