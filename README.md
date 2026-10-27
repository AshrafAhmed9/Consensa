# Consensa

[![CI](https://github.com/AshrafAhmed9/Consensa/actions/workflows/ci.yml/badge.svg)](https://github.com/AshrafAhmed9/Consensa/actions/workflows/ci.yml)

A small distributed vector database in Go. Raft is written by hand (no consensus library),
vector search is an HNSW index written from scratch, and clients talk to it over gRPC.

A client upserts a vector. A majority of replicas log it durably through Raft, then every
replica applies it to its own HNSW graph. Search reads the local graph.

## What's in here

| Path | What it is |
|---|---|
| `internal/raft` | Raft: leader election with pre-vote, log replication, learners, joint-consensus membership changes. The algorithm is a pure state machine; `Host` adds TCP and disk. |
| `internal/storage` | Tiny key-value store (a checksummed log file plus an in-memory map) used as Raft's durable log. |
| `internal/ann` | HNSW index and `DurableNode`, which applies Raft-committed inserts and deletes to it. A restarted node rebuilds its graph by replaying its own log. |
| `internal/server`, `api/` | The gRPC service: `Upsert` (streaming), `Search` (streaming), `Delete`, `Status`. |
| `cmd/consensa` | The node binary. |
| `cmd/democlient` | Small gRPC client used by `demo.sh`. |
| `cmd/torture`, `harness/torture` | Fault-injection runs against a Raft cluster plus a linearizability checker. |
| `cmd/annbench`, `harness/bench` | Recall benchmark against brute-force ground truth. |
| `specs/` | TLA+ models checked with TLC. |

## Demo

```sh
./demo.sh
```

One command, about 15 seconds. It starts a real 3-node cluster (three OS processes), stores
and searches vectors, kills a node with SIGKILL and keeps writing, restarts it and shows it
serving the data again, then runs the linearizability check and the recall benchmark. It
needs `go` and `python3` with `numpy`. The TLA+ step needs `java` and `tla2tools.jar` in
`specs/` (not committed; download it from the
[TLA+ releases](https://github.com/tlaplus/tlaplus/releases)), or `TLA_JAR=/path/to/it`.
It is skipped if either is missing.

## Run a node by hand

```sh
go run ./cmd/consensa -id 1 -peers "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003" \
  -data-dir /tmp/consensa1 -grpc-listen :8081 -dimension 3
# repeat for -id 2 and -id 3 with their own -data-dir and -grpc-listen
```

Writes only succeed on the current leader; a client retries across nodes until one accepts
(`cmd/democlient` does this).

## What is verified

**Linearizability under faults.** `cmd/torture` drives an in-process Raft cluster under a
seeded schedule of node isolations and records the client-visible read/write history.
`harness/torture/checker/linearizability.py` checks that history. The same seed always gives
the same schedule, so a failing seed can be rerun.
`python3 -m harness.torture.cli run --nemesis partition,crash --seeds 100`

Limits: this is an in-process cluster, not separate OS processes. "Partition" and "crash"
are both modeled as full isolation of one node, so a crashed node does not lose or recover
state in this harness. Real process kill and restart is covered separately by
`cmd/consensa/main_e2e_test.go`, which runs three real processes, kills one and restarts it.
`internal/ann/durable_test.go` checks a node recovers its index from its own disk alone.

**TLA+.** `specs/membership.tla` checks that during joint consensus two different candidates
can never both win the same term (quorum intersection). `specs/split.tla` checks that
repeated range splits never leave a key unowned or owned twice. (The server does not shard
yet, so that one is a model only.) Both are small models of
one property each; `specs/README.md` says what they leave out. Run with
`make -C specs check TLA_JAR=/path/to/tla2tools.jar`.

**Search quality.** On 5,000 synthetic 128-dimension vectors (20 Gaussian clusters),
HNSW at efSearch=64 returns 98.6% of the true top-10 with a 156 µs mean search, measured
against exact brute-force search. The data is synthetic, not a real embedding set. Method
and the full sweep are in `docs/benchmarks/04-ann.md`.

## Known limits

- Approximate search is checked for recall, not linearizability. Only the replicated
  mutation order is linearizable.
- There is no runtime sharding; every node holds the whole index.
- The log is never compacted or snapshotted, so it only grows.
- Inserts and deletes compare against every stored vector, so building a large index is slow (5,000 vectors takes about 2 s). Search does not have this cost.
- Reads are served from the local replica and can lag the leader.
- No auth, TLS or metrics.

## Tests

```sh
go vet ./...
go test -race -p 1 ./...
python3 -m pytest harness
```
