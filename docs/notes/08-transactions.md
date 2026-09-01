# Phase 8: transactions

## Why does this exist?

An ingest batch spanning ranges must not become partially visible after a failure.

## How does it work?

An HLC assigns a timestamp. Participants write intents, then a record on the first
participant is the sole commit point. A reader that meets an intent consults that anchor:
a committed record exposes the intent immediately, while cleanup can occur later.

## What alternatives existed?

Independent per-range upserts are cheaper but cannot offer atomic visibility. A full
serializable protocol is stronger but requires conflict tracking beyond this phase.

## What tradeoff was made?

This is snapshot isolation: it keeps intent resolution explicit and testable, while Phase
14 later adds read tracking and refresh to eliminate write skew.

## What can fail?

**Update: the coordinator's decisions are now Raft-persisted, not just modeled.**
`Coordinator` was refactored to depend on a `Participant` interface instead of the
concrete in-memory `Store`, and `DurableStore` (`internal/txn/durable_store.go`)
implements it by proposing every record and intent through a real `kv.DurableRange` --
the identical 2PC logic in `coordinator.go`, unmodified, now runs over durable, replicated
storage. `TestCoordinatorCommitsAcrossRealRaftRanges` proves a transaction committing
across two genuinely separate 3-node Raft groups; `TestDurableStoreRecordSurvivesRestart`
proves a transaction record and its resolved value survive a real node restart, recovered
purely from that node's own Raft log. See `docs/adr/008-wire-txn-onto-durable-ranges.md`
for the full account, including a real gap this wiring surfaced (`rangeClient.Put`
returns once locally appended, not once committed, which the coordinator's protocol logic
implicitly assumed would already be visible -- closed with a confirm-on-write helper, not
by weakening the coordinator's contract).

**Update: a client-facing RPC now drives Coordinator.** `ConsensaKV.TransactionalPut`
(`api/consensa/v1/consensa.proto`, implemented in `internal/server/kv_service.go`) takes a
map of key/value writes, resolves each key's owning range through a real `kv.Router`,
groups them into one `txn.WriteSet` per range, and commits them all atomically through a
real `txn.Coordinator`.
`TestTransactionalPutCommitsAcrossRealRangesOverGRPC` proves the whole path a client
actually uses: a real gRPC call reaching two real, independent 3-node `kv.DurableRange`
groups, committed atomically. This is deliberately a separate gRPC service from
`Consensa` (the vector-index plane) rather than folded into it -- see
`kv_service.go`'s doc comment for why mixing the two would be the wrong shape, not a
convenience worth taking.

`cmd/consensa` now starts the vector group and two static KV ranges behind one shared
Raft listener, then registers `KVService` alongside the vector gRPC service.
`TestConsensaBinaryThreeProcessClusterSurvivesKillAndRestart` proves a real client can
commit keys on both sides of the split through three separate OS processes before it
exercises vector failover/recovery. There is still no single-key read/write RPC (every
write goes through the full 2PC path, correct but not the cheapest shape for the common
one-key case) and no automatic retry against `kv.ErrRangeKeyMismatch` the way
`kv.RoutedKV.retryRoute` already models for the non-transactional path. `WriteIntent`'s conflict check also
remains non-atomic with its own bookkeeping update under concurrent writers to the same
key, for the same reason `kv.DurableRange` has no conditional Put to build a real fix on.
