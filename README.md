# Consensa

Consensa is a from-scratch Go distributed vector-storage engine: a hand-built Raft
implementation, a durable MVCC storage engine, an HNSW approximate nearest-neighbour
index, cross-range 2PC transactions, and a seeded chaos-testing harness that drives all
of it under real fault injection. The build plan is in [PLAN.md](PLAN.md).

## Current runnable surface

Each node is a real OS process with its own on-disk storage and a real TCP Raft
transport -- not an in-memory demo. Run a 3-node cluster by hand:

```sh
go run ./cmd/consensa -id 1 -peers "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003" \
  -data-dir /tmp/consensa1 -grpc-listen :8081 -metrics-listen :9091 -dimension 3
# repeat for -id 2 and -id 3 with their own --data-dir/--grpc-listen/--metrics-listen
```

or bring up a real 3-node cluster plus Prometheus and Grafana in Docker:

```sh
docker compose -f deploy/docker-compose.yml --profile demo up --build
# Grafana at http://localhost:3000 (anonymous admin, demo-only), Prometheus at :9090
# kill and recover one node live:
docker compose -f deploy/docker-compose.yml kill consensa3
docker compose -f deploy/docker-compose.yml up -d consensa3
```

The protobuf contract is at `api/consensa/v1/consensa.proto`: streaming `Upsert`,
streaming `Search`, `Delete`, `BatchGet`, `Status`. A client that reaches a non-leader
replica gets a real `"not leader"` error and is expected to retry elsewhere itself --
this codebase deliberately does not implement server-side leader forwarding (see
`docs/notes/05-api.md`).

## Verification

```sh
go build ./...
go vet ./...
go test ./...
go test -race ./...
python3 -m pytest harness
```

`docs/correctness.md` is the flagship document: what is proven, by which test, and what
is explicitly not established yet. `docs/bugs/` has one file per real bug the test suite
or the torture harness actually found, with its root cause and fix. `docs/adr/` records
the design decisions and their reasoning, including ones later sessions overturned.

## What is actually proven, not just built

- **Real transport, real failover.** Three `Host` replicas over real loopback TCP elect a
  leader, replicate, and recover a majority after a leader's process is killed.
- **Real durability.** A killed-and-restarted `DurableNode` (vector index) or
  `DurableRange` (byte KV) recovers entirely from its own on-disk Raft log, before
  exchanging a single message with a peer.
- **The actual shipped binary**, not just the library: `cmd/consensa`'s own end-to-end
  test builds the real binary, runs three real OS processes, kills one, and confirms
  both the surviving majority and the recovered process behave correctly over real gRPC.
- **Real chaos testing.** `harness/torture` drives real `internal/raft.Cluster` state
  under seeded fault schedules (via `cmd/torture` for the KV/register plane and
  `cmd/vectortorture` for the HNSW/vector plane) and checks the resulting real,
  client-observable history -- not a fixed, seed-independent stub.
- **Real cross-range transactions.** `internal/txn`'s 2PC coordinator commits atomically
  across two genuinely separate 3-node `DurableRange` Raft groups, and a transaction
  record survives a real node restart via real Raft log replay.
- **A live range split preserves search correctness**, proven against real 3-node
  `DurableNode` groups: no vector lost or duplicated, no cross-boundary search leakage,
  recall staying high after the split.
- **Formally verified quorum-intersection and split-invariant properties.** `specs/`
  holds TLA+ models for joint-consensus quorum intersection and recursive range
  splitting, each checked by TLC with a required negative control that must fail.
- **Real metrics.** `consensa_raft_term` and `consensa_range_qps` report real, live
  values from a running process, scraped by a real Prometheus and rendered on a real,
  auto-provisioned Grafana dashboard -- verified against a live Docker cluster, not read
  from source.

## What is explicitly not done yet

Stated plainly rather than left implied by omission: no automatic range-split trigger or
live traffic cutover; no multi-range transport batching beyond a shared listener (every
range still dials its own outbound connections); no joint-consensus membership changes
wired into the live voting path (the quorum math exists and is unit-tested,
`internal/raft/membership.go`, but nothing calls it yet); no client-facing RPC for
cross-range transactions (`internal/txn` is proven durable but not reachable over gRPC);
no recall metric wired to a running process; no serializable isolation (Phase 14 --
snapshot isolation's write-skew gap is reproduced as a test, not yet closed). ANN search
is approximate by construction and is never described as linearizable; only the
register/KV plane makes that claim, and only where a test backs it.
