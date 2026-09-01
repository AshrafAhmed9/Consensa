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
- **The split-trigger decision is real for the KV plane.** `ShouldSplit`/
  `DurableRange.MaybeSplitKey` decide when a range has grown past a size threshold and
  pick the median key of its real applied data as the split point; a real 3-node group
  growing past threshold and having that trigger-chosen key drive the same proven
  migration pipeline above is checked directly
  (`TestMaybeSplitKeyDrivesARealLiveSplit`). This is the decision only -- no timer or QPS
  counter calls it automatically, and no live traffic cutover exists (this project has no
  dynamic routing update path yet) -- see `docs/notes/12-split-repair.md`.
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
- **Learners are wired into the live quorum path, not just modeled.** `Config.Learners`
  marks peers that replicate but never vote and are never counted toward `quorum()`;
  `TestCommitNeverAdvancesOnLearnerAcksAlone` proves the actual safety property directly --
  a leader plus only a learner acknowledging an entry (a literal majority of all nodes) is
  not enough to commit, since it isn't a majority of real voters. Deliberately scoped
  smaller than full joint consensus (see `docs/adr/010-learners.md`): additive and opt-in,
  every existing caller gets byte-identical behavior. Full joint-consensus quorum math
  (`internal/raft/membership.go`) remains unwired.
- **Formally verified quorum-intersection and split-invariant properties.** `specs/`
  holds TLA+ models for joint-consensus quorum intersection and recursive range
  splitting, each checked by TLC with a required negative control that must fail.
- **Real follower reads, the full read-path ladder closed end to end.** Lease grants,
  closed-timestamp advancement, and `DurableRange.FollowerRead` are all real,
  Raft-replicated operations proven against a real 3-node group:
  `TestFollowerReadServesOnceLeasedAndClosed` checks every rejection path (no lease; lease
  but no closed timestamp; a replica that isn't the lease's intended holder even once
  caught up) as well as the success path -- a follower answering a read entirely from its
  own local storage, no leader round trip. Still missing: lease revocation on leadership
  change, a production policy for how often to advance the closed timestamp, and a
  client-facing RPC for it -- see `docs/notes/09-leases.md`.
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

Stated plainly rather than left implied by omission: the split-trigger *decision* is real
for the KV plane (size threshold only, no QPS trigger), but nothing calls it
automatically and there is no live traffic cutover on either plane; outbound connections
are now pooled per destination, but multiple
ranges' messages are still never coalesced into a single wire frame; learners are wired into
the live quorum path (`docs/adr/010-learners.md`), but full joint-consensus membership
changes (dual-majority during a transition) are not -- the quorum math exists and is
unit-tested, `internal/raft/membership.go`, but nothing calls it yet, and there is no
live promotion path (adding/removing a learner from a running group without a restart); serializable isolation is partially closed (Phase 14 -- both `Store` and `DurableStore`
now reject the specific write that completes a reproduced write-skew anomaly,
conservatively, without full SSI's permissive schedule analysis or read-refresh; see
`docs/notes/14-serializable.md`); follower reads work end to end at the `DurableRange`
layer but no running binary calls `AdvanceClosedTimestamp` on a real interval yet, and
lease revocation on leadership change is not implemented. ANN search is approximate by
construction and is never described as linearizable; only the register/KV plane makes
that claim, and only where a test backs it.
