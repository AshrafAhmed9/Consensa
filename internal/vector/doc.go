// Package vector defines the validated, portable vector values used by Consensa's future
// ANN index. It provides scalar distance kernels first because they are a transparent
// correctness baseline for any later architecture-specific optimization. It deliberately
// does not contain an index or collection metadata.
package vector
