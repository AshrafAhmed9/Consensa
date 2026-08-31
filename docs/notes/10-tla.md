# Phase 10: formal models

## Why does this exist?

Membership and splits have intermediate states that ordinary happy-path tests rarely cover.

## How does it work?

The TLA+ modules state leader uniqueness and exact key ownership as invariants over compact
state transitions. They are intentionally separate from Go to make the protocol argument
reviewable.

## What alternatives existed?

More simulator tests find implementation failures but do not exhaust a small abstract state
space or state the safety property independently.

## What tradeoff was made?

The models omit implementation detail to remain finite and understandable. That makes them
a protocol model, not a proof that the implementation conforms.

## What can fail?

An invariant that omits the joint dual-majority rule is too weak to establish membership
safety — this happened during development. The first version of `membership.tla` had an
`Elect` action that didn't require a majority at all (`leaders' = {n}` on any single vote),
so `NoTwoLeaders` (cardinality ≤ 1) was true by construction and could never be violated —
TLC would have "passed" a proof of nothing. Running it and reading the state count (or
lack of meaningful branching) is what catches this, which is why `make check` in `specs/`
now fails loudly if the required negative control (`membership_broken.tla`) ever passes.

TLC has since been run (see `specs/README.md` for exact constants and state counts):
`membership.tla` holds for all three phases (old/joint/new) — quorum intersection under
dual majority genuinely prevents two leaders in the same term, verified rather than
assumed. `membership_broken.tla` (majority-of-Old OR majority-of-New instead of AND)
fails in 7 steps with a committed counterexample — two disjoint 2-of-3 quorums electing
different leaders. `split.tla` was similarly too trivial in its first version (a single
non-recursive split whose invariant was baked into the split action's own precondition,
so it also couldn't fail); it now allows re-splitting any existing range, and TLC explores
every recursive partition of a 3-key space without finding a gap or overlap.

Also still true: this proves the protocol model, not the Go implementation. The Go raft
package (`internal/raft`) and the future joint-consensus and split code (Phases 11–12)
must be read against this argument by a person — TLC does not check Go source.
