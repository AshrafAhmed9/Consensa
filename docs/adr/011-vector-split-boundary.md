# ADR 011: vector-plane split boundary — measured gap, decision, and what's still open

## Context

`ann.ShouldSplit` (`internal/ann/split_decision.go`) picks a split point at the
lexicographic median of vector IDs — the same boundary rule `kv.ShouldSplit` uses for
ordinary keys. That rule has no relationship to embedding-space proximity. Real primary
keys (UUIDs, content hashes) are not correlated with which vectors are semantically
close, so an ID-median cut can (and typically does) slice through clusters arbitrarily.

`ExecuteLiveSplit` (`internal/ann/execute_split.go`) currently handles this by rebuilding
each child's HNSW graph from scratch: every vector that lands in a child is re-`Insert`ed
there through Raft, one at a time, exactly as if it had never been part of a larger
graph. This is PLAN.md Phase 12's option (a). The doc comment on `ExecuteLiveSplit`
already explains why it does not use `HNSW.Split`/`HNSW.Repair` (`split.go`) instead:
those operate on a graph held in one process's memory and have no way to propagate their
result to the other replicas of a live Raft group — a live split spanning independent
processes has to go through Raft the same way any other mutation does.

PLAN.md's own Phase 12 section names three strategies and states a preference for
incremental repair (dropping cross-boundary edges and re-running neighbor selection on
affected nodes) gated by a recall check, over rebuild-from-scratch or serving a stale
parent during a background rebuild. It asks for the choice to be backed by a measured
recall table, not asserted.

## Measurement

`TestMeasureRecallAcrossRealisticIDSplit` (`internal/ann/split_recall_measurement_test.go`)
supplies that measurement, deliberately with a dataset where IDs are independent of
cluster membership — unlike the existing `TestLiveSplitPreservesSearchCorrectness`, whose
two clusters happen to be ID-prefix-aligned and therefore cannot exhibit the degradation
this ADR is about. 8 clusters, 60 points each, dimension 8, `k=10`, 50 queries, split at
the ID-lexicographic median, each query routed post-split to whichever child holds the
majority of its true top-k (the best a real router can do without embedding-aware
routing):

```
recall@10 before split (whole graph):                            0.998
recall@10 after ID-median split, routed to majority-owning child: 0.622
recall dip:                                                       0.376 (37.7% relative)
```

This confirms the gap `split_decision.go`'s own comment already named is real and
substantial under today's rebuild-from-scratch strategy, not merely theoretical: a query
whose true nearest neighbors straddle the ID cut only sees the fraction of them that
happen to land in the child it gets routed to. Rebuild-from-scratch does not fix this —
it faithfully reproduces whatever cut was chosen. The defect is the boundary choice
(ID-lexicographic, not clustering-aware), not the graph-construction strategy on either
side of it.

## Decision

Confirming PLAN.md's stated preference, with this measurement as the evidence: the
long-term direction is a clustering-aware split boundary (grouping vectors by proximity
before choosing which child owns which, rather than by ID order), with incremental graph
repair as the mechanism for realizing it cheaply per child. Rebuild-from-scratch remains
the correct interim strategy for the reason already documented in `ExecuteLiveSplit`: it
is the only one of the three options that has an already-solved answer to Raft
replication across independent processes. `HNSW.Repair`'s edge-dropping mechanism
(`hnsw.go:221`) is fully built and unit-tested, but it operates on one process's
in-memory graph; making its *output* replicate deterministically and consistently across
every replica of a live Raft group is a genuinely separate design problem — how a repair
step becomes a Raft-committed, deterministically-replayable operation rather than a local
mutation each replica might compute slightly differently — and is not solved by this ADR.

## Consequences

**What this ADR does not do:** it does not implement a clustering-aware split boundary,
and it does not build the replicated version of incremental repair. Both remain real,
scoped, unbuilt future work — PLAN.md's own Phase 12, estimated there at ~4 weeks, for
the reason stated above: the graph-repair mechanism already exists, but making it
replicate safely across a live Raft group is a correctness-critical distributed-systems
design decision (how repair results are made deterministic and Raft-committed) that
deserves its own focused implementation and test pass, not something to bolt on inside an
unrelated change.

**What ships now:** the measured evidence that the gap is real (this ADR), confirming the
existing, honest wording in `docs/correctness.md` and `README.md`'s "known limits"
section, which already state the recall dip as a known, unresolved limitation rather than
implying it is fixed.
