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
claim. Invalid dimensions are rejected. Recall must be evaluated on a pinned corpus before
any performance or accuracy claim is made.

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
