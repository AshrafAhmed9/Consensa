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

## Status

**Until this session, the registry was built and exposed over HTTP but never actually
updated.** `cmd/consensa/main.go` created the `metrics.Registry` and served it at
`/metrics`, but nothing ever called `.Set()` on any of its three gauges -- hitting the
endpoint would always return `consensa_raft_term`, `consensa_range_qps`, and
`consensa_ann_recall` at their permanent zero value, regardless of real cluster activity.
`consensa_raft_term` is now wired for real: the node's already-running tick loop calls
`node.Status()` and sets the gauge from the real term on every tick.
`TestConsensaBinaryThreeProcessClusterSurvivesKillAndRestart`
(`cmd/consensa/main_e2e_test.go`) proves it end to end -- a real HTTP GET against a real
running process's `/metrics`, parsed for the real value, asserted `>= 1` only after real
elections have actually happened.

`consensa_range_qps` and `consensa_ann_recall` are still unwired, deliberately left at
zero rather than filled with a plausible-looking placeholder -- QPS needs request
counting added to `internal/server.Service`, and recall needs a hook from
`harness/bench`'s benchmark path into the running process, neither of which exists yet.
Grafana dashboards, structured JSON logging, and the `docker compose up` demo GIF remain
entirely unbuilt.
