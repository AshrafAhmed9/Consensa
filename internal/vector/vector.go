package vector

import (
	"encoding/binary"
	"errors"
	"math"
)

// Vector is an embedding represented as IEEE-754 float32 components.
type Vector []float32

// ErrDimensionMismatch reports an attempt to compare or store vectors of different dimensions.
var ErrDimensionMismatch = errors.New("vector: dimension mismatch")

// ErrNonFinite reports a component that would make distance ordering undefined.
var ErrNonFinite = errors.New("vector: non-finite component")

// ValidateDimension makes a collection's fixed dimension an explicit API boundary.
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

// Encode produces a length-prefixed representation suitable for an opaque LSM value.
func (v Vector) Encode() []byte {
	b := make([]byte, 4+4*len(v))
	binary.BigEndian.PutUint32(b[:4], uint32(len(v)))
	for i, x := range v {
		binary.BigEndian.PutUint32(b[4+4*i:8+4*i], math.Float32bits(x))
	}
	return b
}

// Decode parses the compact wire representation without silently accepting trailing bytes.
func Decode(b []byte) (Vector, error) {
	if len(b) < 4 {
		return nil, errors.New("vector: truncated dimension")
	}
	n := int(binary.BigEndian.Uint32(b[:4]))
	if n < 0 || len(b) != 4+4*n {
		return nil, errors.New("vector: invalid encoded length")
	}
	v := make(Vector, n)
	for i := range v {
		v[i] = math.Float32frombits(binary.BigEndian.Uint32(b[4+4*i : 8+4*i]))
	}
	return v, nil
}
