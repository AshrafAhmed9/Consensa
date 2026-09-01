# Phase 4: approximate nearest-neighbour indexes

## Why does this exist?

Brute-force vector search is exact but scales linearly with every stored embedding. HNSW
provides a navigable proximity graph whose search breadth can trade latency for recall.

## How does it work?

Each vector receives a deterministic random level. At each layer, candidate neighbours
are selected with the HNSW diversity rule: a candidate that is already closer to an
accepted neighbour than to the inserted node is rejected. This retains useful long links.
Mutations are serialised before Raft replication; snapshots are canonical JSON.

## What alternatives existed?

IVF-Flat groups vectors behind centroids and scans selected lists; it is included as a
baseline. Rebuilding graph topology from stored vectors is smaller on disk but makes
replica equality dependent on repeatable graph construction.

## What tradeoff was made?

This first graph favors deterministic, inspectable behavior over compact binary layout and
maximum throughput. Its scalar distance and canonical snapshots are the reference point
for later optimisation.

## What can fail?

ANN search is approximate and therefore measured by recall rather than a linearizability
claim. Invalid dimensions are rejected.

**The pinned-corpus recall harness now exists and is connected to the real Go index**,
closing a gap this file used to just flag as future work. `cmd/annbench` (a small Go CLI)
builds a real `internal/ann.HNSW` or `IVFFlat` and prints its search results as JSON;
`harness/bench/run_recall_benchmark.py` computes an independent brute-force ground truth
in NumPy and measures actual recall@10 by comparing the two -- not a fabricated or assumed
number. `harness/bench/test_recall_regression.py` pins the seed, dataset shape, and HNSW
parameters and asserts recall stays within a tolerance band of a committed baseline, which
is what makes the gate deterministic instead of a flake generator (PLAN.md's own warning
about HNSW's randomized level assignment). Results and the exact reproduction command are
in `docs/benchmarks/04-ann.md`: recall@10 clears the plan's 0.95 target at efSearch=32 on a
5,000-vector synthetic dataset.

**Still an open question, stated precisely now rather than generally:** the dataset is
synthetic (Gaussian clusters), not a real embedding corpus (SIFT-1M or similar) -- the
harness is real and reusable, but has not yet been pointed at real data. IVFFlat's
centroids are also not k-means-trained (`cmd/annbench` seeds them as the dataset's first N
vectors), so the HNSW-vs-IVFFlat comparison in the benchmarks doc is against a simple
baseline, not IVFFlat's actual ceiling.

**Durability was an open question until this session, and the answer turned out not to
need any HNSW-specific code.** `ReplicatedIndex` (the composition used by every existing
test and by `cmd/consensa/main.go`) is explicitly, deliberately in-memory only — its own
doc comment says so. `DurableNode` (`durable.go`) is the actual answer: it wires the same
`HNSW.ApplyMutation` as the `Apply` callback of a real `raft.Host` backed by a real
`storage.Engine`. Because `Persister` already durably logs every committed Raft entry and
`RecoverNode` restores `committed` from disk while `applied` starts from zero (or the last
snapshot), the very first `Ready()` after a restart replays the *entire* committed
mutation history straight into a freshly constructed graph — recovering it exactly the way
it was built the first time, deterministically, with zero bytes of graph-specific
persistence format. `TestDurableNodeSurvivesRestart` in `durable_test.go` proves this: kill
one of three real-TCP replicas, restart it from the same directory, call `Tick()` once
(the local-only nudge needed to flush the recovered backlog through `Apply` — see the
`Host` doc comment on why nothing happens automatically at construction), and confirm a
correct nearest-neighbour search — before the restarted node has exchanged a single
message with either surviving peer.
