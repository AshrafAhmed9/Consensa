# Phase 13: observability

## Why does this exist?

Correctness failures and ANN recall regressions need evidence that survives a demo run.

## How does it work?

The metrics package exposes a node-local Prometheus registry for Raft term, range load,
and measured recall. Storage exports operation counters and latency histograms separately.

## What alternatives existed?

Ad hoc logs make one incident readable but cannot support dashboards or regression trends.

## What tradeoff was made?

Only bounded-cardinality metrics are introduced. Range- and node-level labels are safe;
client keys, vector IDs, and transaction IDs are not metric labels.

## What can fail?

Metrics report measurements, not guarantees. A recall gauge is meaningful only when the
benchmark corpus and query set are pinned and documented.
