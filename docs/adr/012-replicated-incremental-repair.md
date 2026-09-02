# ADR 012: replicated incremental repair for the vector split boundary

## Context

`docs/adr/011-vector-split-boundary.md` measured the real cost of the vector plane's
ID-lexicographic split boundary: recall@10 drops 37.7% relative under rebuild-from-scratch
(`ExecuteLiveSplit`'s strategy at the time). It confirmed PLAN.md's stated preference for
incremental repair over rebuild, but explicitly deferred the actual implementation,
naming the open design problem precisely: `HNSW.Repair`'s edge-dropping mechanism already
existed and was unit-tested, but operated only on one process's in-memory graph. Making
its *output* replicate deterministically and consistently across every replica of a live
Raft group -- rather than the graph bytes themselves needing to be replicated -- was left
unsolved.

## Decision

**The repair operation is now a single Raft-committed instruction, applied
independently and deterministically by every replica**, not a graph that gets
replicated after being computed once. Concretely:

- `ann.Mutation` gained a `"repair"` operation (`persist.go`) carrying the *parent's own*
  canonical snapshot bytes (`HNSW.Snapshot`'s existing format) plus the `[Start, End)`
  bounds the receiving replica should keep.
- `ApplyMutation`'s `"repair"` case calls `Restore` (replace this replica's graph with the
  parent's) then `Repair` (prune to the child's own boundary) -- both already-existing,
  already-tested primitives, now invoked from the Raft apply path instead of only from
  `HNSW.Split`'s single-process convenience wrapper.
- This is safe to compute independently per replica, without replicating the resulting
  graph, *only if* `Restore` and `Repair` are pure, deterministic functions of their
  inputs. `Restore` already was. `Repair` was not, quite: its entry-point re-pick on tie
  (`for id, n := range h.nodes { if n.level > h.maxLevel {...} }`) iterated a Go map,
  whose order is randomized per process -- a real latent bug, invisible until something
  actually needed cross-replica determinism from `Repair`. Fixed by iterating a sorted ID
  list and breaking ties on the lowest ID.
- `ann.ExecuteLiveSplitByRepair` (`execute_split.go`) is the new split-execution entry
  point: one `ProposeRepair` call per child (retried until every replica's applied vector
  count matches expectation, mirroring `ExecuteLiveSplit`'s own `insertAndConfirm`
  pattern), instead of one `Insert` proposal per vector. `cmd/consensa` now calls this
  instead of `ExecuteLiveSplit` for the vector plane
  (`TestConsensaBinaryExecutesALiveVectorSplitAutomatically` still passes against the
  real binary). `ExecuteLiveSplit` itself is kept, not removed: it is still correct, and
  is what a caller without a recent parent snapshot must fall back to.
- `TestExecuteLiveSplitByRepairMigratesRealVectors` proves this against real 3-node Raft
  groups on both planes' worth of correctness (no vector lost, duplicated, or leaked
  across the boundary) *and* the property unique to this design: every replica of a child
  group ends up with a **bit-identical** graph (`reflect.DeepEqual` on `Snapshot()`
  output across all three replicas), not merely the same vectors.

## A real finding along the way: repair alone was worse than rebuild

`TestMeasureRecallRepairVsRebuildAcrossRealisticIDSplit` first measured `HNSW.Repair` as
it existed before this ADR -- drop cross-boundary edges, retrim -- against the same
realistic (ID-independent-of-cluster) dataset ADR-011 used:

```
recall@10 before split (whole graph):        0.998
recall@10 after split, rebuild-from-scratch:  0.622
recall@10 after split, incremental repair:    0.396   <- worse than rebuild
```

The reason: `Repair`'s filter/trim pass can only ever *shrink* a neighbor list (drop
edges pointing at now-absent nodes); it never searches for replacements. A node that had
most of its M neighbors on the other side of the cut is left severely under-connected,
which hurts search quality more than a full rebuild -- where every node gets a fresh,
complete neighbor list from scratch. PLAN.md's own phrasing ("dropping cross-boundary
edges **and re-running neighbor selection on the affected nodes**") already named the
missing half; `Repair` implemented only the first part.

**Fix:** `Repair` now backfills. After the drop/retrim pass, any node whose neighbor list
at a level falls under `M` searches the *already-pruned* graph (`closestNotIn`, the
multi-exclude counterpart of the existing `closestExcept`) for replacement candidates and
links them in reciprocally -- the same `selectDiverse` + mutual-link pattern `Insert`
already uses, applied here to previously-existing nodes instead of a newly-inserted one.
With backfill:

```
recall@10 after split, rebuild-from-scratch:  0.622
recall@10 after split, incremental repair:    0.592   (-4.8% relative to rebuild)
```

## Consequences

**Honestly, repair still does not clearly beat rebuild on recall** in this measurement --
it comes within 5% relative, not ahead. What it wins instead: **replication cost.**
Rebuild proposes one Raft entry per vector (`O(n)` log entries, `O(n)` round-trips through
`insertAndConfirm`'s per-key confirm-visible polling); repair proposes exactly one entry
per child regardless of size. For a range with thousands of vectors this is the
practically significant win, not recall -- and it comes at recall parity rather than a
recall cost, which is what makes it the right default despite not "winning" the number
ADR-011 set out to close.

**What this ADR does not do:** it does not change the split *boundary* itself
(ID-lexicographic, not clustering-aware) -- ADR-011's finding that the boundary choice,
not the graph-construction strategy on either side of it, is the actual defect still
stands, and remains real, unimplemented future work. `docs/correctness.md` and
`README.md` are updated to describe this as the current, honest state: a replicated,
deterministic, single-Raft-entry repair mechanism now exists and is in the live split
path, at measured recall parity with the rebuild it replaced, while the underlying
boundary-quality problem is unchanged.
