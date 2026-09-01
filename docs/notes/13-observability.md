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

**`consensa_range_qps` is now wired too.** `server.Service` counts every data-plane RPC
(`Upsert`, `Search`, `Delete`, `BatchGet`) it receives -- including ones that fail, since a
QPS metric should measure load received, not requests that happened to succeed
(`TestRequestCountIncrementsAcrossRPCs` proves this). A separate 1-second-window loop in
`cmd/consensa/main.go` samples the delta and sets the gauge to a real requests-per-second
value, deliberately not folded into the Raft term's per-tick loop: a rate needs a fixed
sampling window, an instantaneous value like the Raft term does not.

`consensa_ann_recall` remains unwired, deliberately left at zero rather than filled with a
plausible-looking placeholder -- it needs a hook from `harness/bench`'s benchmark path
into the running process, which does not exist yet.

**Grafana dashboards are now provisioned and verified against the real stack, not just
written.** `deploy/docker-compose.yml` gained `prometheus` and `grafana` services;
`deploy/prometheus/prometheus.yml` scrapes all three real nodes' `/metrics`, and
`deploy/grafana/provisioning/` auto-loads a Prometheus data source and a
`consensa-overview` dashboard with zero manual setup. Verified end to end with a real
`docker compose --profile demo up --build`, not just YAML review: all three Prometheus
targets came up healthy; `consensa_raft_term` queried real `1`s from a real 3-node
election; the dashboard's three panels loaded exactly as provisioned; and — the part that
actually proves the pipeline, not just the plumbing — issuing 20 real gRPC `Search`
requests against one node made `consensa_range_qps` report a real, nonzero rate
(`9`) on exactly that node for several seconds while the other two, which received no
traffic, correctly stayed at `0`. The dashboard's third panel is a text panel stating
plainly that recall is not measured yet, rather than shipping a graph that could only ever
show a flat zero line.

Structured JSON logging and the `docker compose up` demo GIF remain entirely unbuilt.
