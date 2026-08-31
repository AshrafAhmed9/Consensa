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
