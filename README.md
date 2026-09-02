# Consensa

A distributed vector database, built from scratch in Go: a hand-written Raft consensus
implementation, a durable MVCC storage engine, an HNSW approximate-nearest-neighbor
index, cross-shard transactions, and a chaos-testing harness that attacks all of it with
real process kills and network partitions. No consensus library, no embedded database,
no vector-search library — the algorithms themselves are the project.

Every node in the diagrams below is a real OS process. Every claim in this README is
backed by a test that runs against real TCP sockets and real killed processes, not an
in-memory simulation standing in for one.

## What it does

Think of it as "Pinecone, but you can see every line of the replication logic." A client
sends a vector; three (or more) replicas agree on it via Raft, write it durably to disk,
and index it for nearest-neighbor search — so that killing any one replica's process
loses no data and drops no in-flight query.

```mermaid
flowchart TB
    Client["Client app<br/>RAG pipeline / gRPC client"]
    API["gRPC API<br/>Upsert · Search · TransactionalPut"]
    Router["Router<br/>which shard owns this key?"]

    Client --> API --> Router --> Leader

    subgraph Range["One shard = one 3-node Raft group"]
        direction LR
        Leader["Node 1 — Leader<br/>Raft + HNSW + MVCC storage"]
        Follower2["Node 2 — Follower"]
        Follower3["Node 3 — Follower"]
        Leader ---|replicate| Follower2
        Leader ---|replicate| Follower3
    end

    Leader --> Applied["Durable storage (WAL + SSTables)<br/>+ HNSW search graph"]

    Split["Range splitter<br/>size threshold"]
    Torture["Chaos harness<br/>kills nodes, cuts network"]
    TLA["TLA+ specs<br/>proves the protocol on paper"]

    Split -. splits .-> Range
    Torture -. attacks nightly .-> Range
    TLA -. proves safety .-> Range
```

A write only succeeds once a majority of a shard's replicas have durably logged it. A
read from the leader is always linearizable; a bounded-staleness read from a follower is
available once that replica holds a valid lease. Vector search is approximate by
construction (that's what makes it fast at scale) — it is never described as
linearizable, and the KV/metadata plane is the only part of this project that makes that
claim.

## Try it

```sh
./demo.sh
```

brings up a real 3-node cluster, upserts and searches vectors, commits a cross-range
transaction, kills a node's real process, shows the surviving majority still answers
correctly, then restarts the killed node and shows it recovers from its own on-disk log
-- printing real Prometheus metrics scraped from a live process at the end. It uses
[`cmd/democlient`](cmd/democlient), a small real gRPC client, not a mock.

Or run nodes by hand:

```sh
go run ./cmd/consensa -id 1 -peers "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003" \
  -data-dir /tmp/consensa1 -grpc-listen :8081 -metrics-listen :9091 -dimension 3
# repeat for -id 2 and -id 3 with their own --data-dir/--grpc-listen/--metrics-listen
```

or the whole cluster plus a live Grafana dashboard in one command:

```sh
docker compose -f deploy/docker-compose.yml --profile demo up --build
# Grafana: http://localhost:3000 (anonymous admin, demo-only) · Prometheus: :9090
docker compose -f deploy/docker-compose.yml kill consensa3   # kill a node live
docker compose -f deploy/docker-compose.yml up -d consensa3  # watch it recover
```

## What's actually proven

Every bullet below is backed by a specific, named test that runs against real replicas —
not asserted, not just "should work." The full account, including exactly what each test
does and doesn't cover, is in [`docs/correctness.md`](docs/correctness.md).

| Area | What's proven |
|---|---|
| **Consensus** | Hand-built Raft with pre-vote and the Figure 8 commit-safety rule, each closed by a scripted test that fails if the rule is weakened — not just "the happy path works." |
| **Durability** | A killed-and-restarted replica recovers its full state from its own on-disk log alone, before contacting a single peer — proven for both the vector index and the KV store. |
| **The shipped binary** | An end-to-end test builds the real `consensa` binary, runs three real OS processes, kills one mid-write, and checks the survivors and the recovered process over real gRPC. |
| **Chaos testing** | A seeded Python harness drives real partitions, process kills, and clock skew against real Raft clusters and checks the resulting history for linearizability (via [Porcupine](https://github.com/anishathalye/porcupine)) and search-result correctness. |
| **Cross-shard transactions** | A 2PC coordinator commits atomically across two independent 3-node Raft groups, reachable over a real gRPC call, and survives a coordinator crash mid-commit. |
| **Live range splits** | A shard splits into two fresh replica groups with no vector or key lost, duplicated, or leaking across the new boundary — proven on both the KV and vector planes, including publishing the new routing metadata atomically so both a fresh client and one holding a pre-split cached route resolve correctly afterward. Both planes now do this fully automatically inside the running binary, not just as a library primitive — real writes crossing a threshold trigger real child Raft groups standing up and taking over live traffic. |
| **Joint-consensus membership changes** | Adding, promoting, or removing a replica goes through Raft's two-phase joint-consensus protocol, so a disjoint old/new majority can never both elect a leader — the specific failure mode joint consensus exists to prevent is covered directly. This now includes provisioning a genuinely new, previously-unknown process, reachable over a real, optionally auth-gated `ConsensaAdmin` gRPC surface, not just an internal Go primitive. |
| **Read-path ladder** | Leader reads via a quorum-confirmed barrier (`ReadIndex`); follower reads via a replicated lease and closed timestamp, rejected until both are actually valid — not just modeled. |
| **Write-skew prevention** | The classic "two on-call doctors" anomaly is reproduced and the specific write that would complete it is rejected, on both the in-memory and Raft-replicated code paths; a transaction pushed by that check can also read-refresh and commit anyway instead of always aborting, proven on both paths too. |
| **Formal verification** | TLA+ models of joint-consensus quorum intersection and recursive range splitting, model-checked by TLC — each with a deliberately broken variant that must fail, so the checker itself is proven discriminating. |
| **Observability** | Real Prometheus metrics (Raft term, QPS, recall, and now the split-trigger recommendation) from a live process, rendered on an auto-provisioned Grafana dashboard, recall computed by an external client against an independent ground truth. |

`docs/bugs/` has a write-up for every real bug the test suite or chaos harness actually
found — root cause, fix, seed, regression test. `docs/adr/` records the design decisions,
including the ones later sessions overturned.

## What's inside

- **`internal/raft`** — Raft from the paper: pre-vote, the Figure 8 commit rule, snapshots,
  learners, and full joint-consensus membership changes, as a pure state machine with no
  goroutines or I/O inside the algorithm itself.
- **`internal/storage`** — an LSM-tree key/value engine: WAL, skiplist memtable, sorted
  SSTables, MVCC versioning by hybrid-logical-clock timestamp.
- **`internal/ann`** — HNSW, including the neighbor-selection heuristic that makes it
  outperform naive top-M graphs, plus the split-and-rebuild strategy that keeps search
  correct after a shard splits.
- **`internal/kv`** — range sharding, routing, and the split-trigger decision.
- **`internal/txn`** — HLC, write intents, a two-phase commit coordinator, and the
  timestamp-cache defense against write skew.
- **`api/consensa/v1`** — the gRPC contract: streaming `Upsert`/`Search`, `TransactionalPut`.
- **`harness/`** — the Python chaos-testing control plane and checkers.
- **`specs/`** — TLA+ models of the two hardest correctness properties.

## What's not done yet

Stated plainly rather than left implied: `cmd/consensa` now automatically executes a live
split on both planes -- standing up fresh child Raft groups at runtime on the same shared
transport, migrating data, and cutting real traffic over -- once each plane's own
split-trigger decision recommends one (`kv.ExecuteLiveSplit`/`ann.ExecuteLiveSplit`,
`TestConsensaBinaryExecutesALiveSplitAutomatically`/`TestConsensaBinaryExecutesALiveVectorSplitAutomatically`).
The vector plane's split boundary is a lexicographic ID bisection, not a clustering-aware
vector-space boundary, so recall near that boundary can dip until each child's own graph
structure compensates -- the minimum viable decision proving automatic execution works
end-to-end, not PLAN.md's own named answer to HNSW-under-splits (incremental repair or a
stale-parent fallback), which remains real, unimplemented future work. There is still no
in-flight request cutover (an RPC already routed to the parent mid-split completes there;
a client re-routes on its next call). The split trigger is no longer size-only: both
planes now also support a QPS threshold (`--split-qps-threshold`, off by default), so a
range that is small by key/vector count but genuinely hot under a skewed access pattern
can still recommend a split -- `kv.ShouldSplit`/`ann.ShouldSplit` fire on either active
criterion, backed by a real per-range request counter and a live rate sampled the same
way the existing QPS metric already was. Joint consensus can now provision a genuinely new, previously-unknown
process too, reachable over a real `ConsensaAdmin.AddReplica`/`PromoteReplica` gRPC
surface -- `cmd/consensa` now exposes it, not just `internal/raft`'s own primitives, and
every RPC across all three services is now gated by an optional shared-secret bearer-token
layer (`--auth-token`, off by default; see `docs/notes/13-auth.md` and `internal/auth`),
with `ConsensaAdmin` independently scopable via `--admin-auth-token` so a leaked
data-plane credential can't drive membership changes -- within each scope, no per-user
identity, no rotation, or transport encryption of its own, stated plainly rather than
left implied. Snapshot isolation now supports
read-refresh (a pushed transaction re-validates its own prior reads instead of aborting
outright), proven for both the in-memory `Store` and the real, Raft-replicated
`DurableStore`; a running binary now advances the closed timestamp and automatically
grants/renews follower-read leases on a
real interval, but no RPC surface exposes follower reads to a network client yet. A
cross-range transaction still only commits through whichever single process leads every
range it touches (no server-side request forwarding), but that process is no longer left
to chance: a real leadership-transfer primitive (`raft.Host.TransferLeadershipTo`, Raft's
own `MsgTimeoutNow`) plus a self-correcting affinity policy now actively converges every
co-located group's leadership onto the same, deterministically-preferred process, fixing
the real, intermittent stable-split failure documented and previously only mitigated in
`docs/bugs/003`. See
`docs/correctness.md` for the complete, current list.

## Verification

```sh
go build ./...
go vet ./...
go test -race -p 1 ./...
python3 -m pytest harness
```

Build plan: [`PLAN.md`](PLAN.md).
