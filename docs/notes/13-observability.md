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

**`consensa_ann_recall` is now wired too.** A node cannot compute its own recall (it needs
a labeled dataset and independent ground truth, neither of which it has reason to know
about), so `cmd/consensa` exposes `POST /report-recall` on the metrics port -- the same
push shape a Prometheus pushgateway uses -- for an external benchmark client to report a
value it already computed against that node's real `Search` RPC.
`TestConsensaBinaryReportsRealRecallMetric` (`cmd/consensa/main_e2e_test.go`) proves the
whole pipeline against a real 3-node cluster: real vectors upserted, an independent
brute-force ground truth computed separately from anything HNSW touches (the same
discipline `cmd/vectortorture`'s `bruteForceTopK` uses), a real `Search` RPC checked
against it, the resulting recall pushed, and read back from `/metrics`.

Building this test surfaced a real lesson about writing this class of test correctly, not
a product bug: an early version's search-retry loop checked only `len(results) == k`
before accepting an answer, not whether the results actually matched ground truth. A
freshly upserted key can take a moment to replicate to whichever node the search happens
to land on, so the loop could exit early with the *wrong* k results (whatever was already
in the graph) and then fail the recall assertion downstream for a reason that looked like
a real bug but was actually the test not waiting for the right condition. Fixed by
retrying until the result content matches ground truth, not just its length -- the same
discipline `internal/ann/durable_split_test.go`'s per-insert `waitForApplied` calls
already established earlier this session for a different reason (racing proposals against
a leadership change). A separate, real finding along the way: proposing many upserts
back-to-back with no pause between them (8, in an earlier version of this test) measurably
increased the chance of hitting a leadership blip long enough to exceed even a generous
per-key retry budget, compared to 3; the test uses a small, deliberately chosen dataset
now, and the underlying question -- why a healthy cluster doesn't always recover a rapid
upsert burst comfortably within the existing retry budget -- is noted here as worth
investigating further, not chased down in this pass.

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
