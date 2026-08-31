# Phase 3: vector values and distance kernels

## Why does this exist?

An ANN index needs a dependable representation and distance baseline before graph choices
are meaningful. These functions become the reference for later optimized implementations.

## How does it work?

Vectors encode their dimension followed by IEEE-754 float32 components. L2 squared, dot
product, and cosine distance use scalar float32 accumulators. The main loop is eight-wide
unrolled and is resliced first so the compiler can eliminate repeated bounds checks.

## What alternatives existed?

A vector library or AVX2 assembly would be faster to claim, but both hide the baseline.
The optional assembly work was deliberately deferred: it is only justified by benchmark
evidence and has a significant maintenance cost.

## What tradeoff was made?

Dimension validation happens at the API boundary, while hot kernels use a documented
precondition. Mismatches return NaN from kernels as an unmistakable invalid result; zero
vectors produce NaN cosine distance because their direction is undefined.

## What can fail?

Float32 arithmetic has rounding error and can overflow at extreme magnitudes. Callers
must validate collection dimensions and reject non-finite input before indexing.
