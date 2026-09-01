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

Also still true: this proves the protocol model, not the Go implementation. **Re-verified
this session**: `specs/Makefile`'s `make check` still passes exactly as documented above
(`membership.tla` 0 violations, `membership_broken.tla` fails in 7 steps with the same
counterexample, `split.tla` 0 violations) — the specs were not just written once and left
untested, they still hold.

The Go implementation now consumes this model's quorum rule directly:
`internal/raft/membership.go`'s `Membership.HasQuorum` is used by election and commit
advancement while a configuration is joint. Configuration entries take effect on append,
are rebuilt after an uncommitted entry is overwritten, and their `ConfState` is carried in
snapshots so compaction and recovery cannot revert a replica to startup membership.
Focused tests cover the old-only partition, overwrite, automatic finalization, removal of
the leader, snapshot restore, and recovery. TLC remains a protocol model rather than a
proof of implementation conformance; the nemesis scenarios in Phase 11 are still useful
integration work.
