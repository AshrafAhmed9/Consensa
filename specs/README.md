# Consensa TLA+ models

Two properties, model-checked with TLC.

## `membership.tla` — quorum intersection during joint consensus

**Property checked:** for a fixed phase (old, joint, or new), can two different candidates
both collect a legitimate quorum for the same term? The joint phase requires a majority of
Old **and** a majority of New simultaneously; a node casts at most one vote per term.
`NoTwoLeaders` holds because any two majorities of the same finite set must share a member
(pigeonhole), and the single-vote rule turns that overlap into a shared voter — that is
the actual safety argument, verified as a consequence of the model rather than assumed by
the invariant.

Checked for `Phase \in {"old", "joint", "new"}` with `Old = {n1,n2,n3}`,
`New = {n2,n3,n4}`, `Terms = {1}`: **0 violations**, 757–885 distinct states per run.

**`membership_broken.tla`** is the required negative control: it replaces the joint
phase's `Majority(Old) /\ Majority(New)` with `Majority(Old) \/ Majority(New)` — the bug
joint consensus exists to prevent. TLC finds a violation in 7 steps: `{n1,n2}` (majority
of Old) elects `n1`, the disjoint set `{n3,n4}` (majority of New) elects `n2`, both in
term 1. The trace is committed at `counterexamples/membership_broken.trace.txt`. If this
file ever passes TLC, the negative control has stopped working and must be fixed before
trusting `membership.tla` again.

**What this does NOT model, stated plainly:** the old → joint → new phase transition
itself, and whether a leader elected before a transition remains valid after it. An
earlier version of this file modeled the transition as free `EnterJoint`/`LeaveJoint`
actions, and TLC correctly found that a leader elected pre-transition and a different
leader elected post-transition (using votes cast fresh under the new phase) can coexist
in the same term. That is a real finding, but it is a different question from quorum
intersection — it depends on how the reconfiguration entry itself gets committed (in real
Raft, the config-change log entry is replicated and committed like any other entry before
the new configuration takes sole effect), which this model does not represent. This spec
proves the quorum-intersection half of the safety argument only.

## `split.tla` — key ownership under recursive range splitting

**Property checked:** starting from one range covering the whole keyspace, any sequence
of splits of any existing range (not just the original) never leaves a key unowned or
double-owned. `Split(r, left, right)` replaces range `r` with `left` and `right` whenever
they partition `r`; `Next` allows re-splitting any range already in the set, which is what
makes this a test of repeated dynamic splitting rather than one static cut.

Checked with `Keys = {k1, k2, k3}`: **0 violations**, 5 distinct states, depth 3 (every
key reachable down to a singleton range).

**What this does NOT model:** concurrent splits racing each other, replica placement
during a split, or the HNSW-graph-repair problem described in `PLAN.md` Phase 12. This
spec proves keyspace partition-invariance only.

## Running the checks

```
cd specs
make check TLA_JAR=/path/to/tla2tools.jar
```

`make check` runs `membership.tla` (must pass), `membership_broken.tla` (must fail — the
target itself errors out if it doesn't), and `split.tla` (must pass), then removes TLC's
state-directory and trace-file artifacts. TLC (`tla2tools.jar`) is not vendored in this
repository; download it from the [TLA+ tools releases](https://github.com/tlaplus/tlaplus/releases)
and pass its path via `TLA_JAR`.
