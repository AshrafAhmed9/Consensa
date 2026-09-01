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
go test -p 1 ./...
go test -race -p 1 ./...
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
- **Real cross-range transactions, reachable over gRPC.** `internal/txn`'s 2PC
  coordinator commits atomically across two genuinely separate 3-node `DurableRange`
  Raft groups, a transaction record survives a real node restart via real Raft log
  replay, and the `ConsensaKV.TransactionalPut` RPC drives all of it from a real network
  client -- not just from Go code in the same process.
- **A live range split preserves correctness on both planes**, each proven against real
  3-node groups by rebuilding fresh child groups from the parent's actual applied data:
  the vector plane (`DurableNode`) shows no vector lost or duplicated, no cross-boundary
  search leakage, and recall staying high after the split; the KV plane (`DurableRange`)
  shows no key lost or duplicated and no cross-boundary key surviving in either child,
  reading the parent's real state via `DurableRange.AllKeys()`.
- **Multi-range outbound connections are pooled, not dialed per message.** Ranges on one
  node sharing a destination now reuse one persistent TCP connection
  (`MultiplexedTransport`'s `connFor`/`pooledConn`) instead of each `Send` dialing fresh --
  `cmd/consensa`'s own shared listener already uses this transport, so the fix applies to
  the real binary. Fixing the receiving side to read many frames per pooled connection
  surfaced and fixed two real bugs: a `bufio.Reader` re-wrapped on every read that
  silently dropped bytes, and cross-range head-of-line blocking from dispatching a
  received frame synchronously off the shared connection's one read goroutine (each range
  now has its own inbox and worker). Coalescing multiple ranges' messages into a single
  wire frame remains out of scope.
- **Formally verified quorum-intersection and split-invariant properties.** `specs/`
  holds TLA+ models for joint-consensus quorum intersection and recursive range
  splitting, each checked by TLC with a required negative control that must fail.
- **Lease grants are real, Raft-replicated state.** `DurableRange.GrantLease` proposes a
  lease through the same replicated command log `Put`/`Delete` use; every replica in a
  real 3-node group converges on the identical holder and interval, checked against
  `lease.go`'s clock-bounded validity logic. Closed-timestamp advancement and an actual
  follower-read RPC path are not built on top of it yet -- see
  `docs/notes/09-leases.md`.
- **A reproduced write-skew anomaly is now actually prevented, not just documented, on
  both participants.** `Store.WriteIntent` and `DurableStore.WriteIntent` both reject a
  write whose timestamp collides with an already-recorded read on the same key;
  `TestWriteIntentRejectsWriteSkew` and its real-Raft counterpart
  `TestDurableStoreRejectsWriteSkew` reproduce the classic two-doctors-on-call scenario
  and prove the specific write that would complete it is rejected, with a control case
  proving unrelated writes are unaffected. This is the conservative reject-and-retry
  response, not full SSI's permissive schedule analysis or read-refresh -- see
  `docs/notes/14-serializable.md`.
- **Real metrics, all three.** `consensa_raft_term`, `consensa_range_qps`, and
  `consensa_ann_recall` all report real, live values from a running process. The first
  two are self-measured; recall is pushed by an external benchmark client that computed
  it against that node's real `Search` RPC and an independent brute-force ground truth
  (a node cannot compute its own recall -- it has no reason to know the labeled dataset
  or ground truth). All three are scraped by a real Prometheus and rendered on a real,
  auto-provisioned Grafana dashboard -- verified against a live Docker cluster and a live
  3-process cluster, not read from source.

## What is explicitly not done yet

Stated plainly rather than left implied by omission: no automatic range-split trigger or
live traffic cutover; outbound connections are now pooled per destination, but multiple
ranges' messages are still never coalesced into a single wire frame; no joint-consensus
membership changes wired into the live voting path (the quorum math exists and is
unit-tested, `internal/raft/membership.go`, but nothing calls it yet); serializable isolation is partially closed (Phase 14 -- both `Store` and `DurableStore`
now reject the specific write that completes a reproduced write-skew anomaly,
conservatively, without full SSI's permissive schedule analysis or read-refresh; see
`docs/notes/14-serializable.md`); lease grants replicate correctly but nothing yet
advances a closed timestamp or serves an actual follower read off one. ANN search is approximate by construction and
is never described as linearizable; only the register/KV plane makes that claim, and only
where a test backs it.
