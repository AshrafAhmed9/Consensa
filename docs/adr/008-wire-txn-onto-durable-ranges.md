# ADR 008: keep MVCC/2PC, and wire it onto real Raft ranges now that sharding exists

## Decision

`internal/txn` (HLC, write intents, transaction records, a 2PC coordinator) stays in
the project. It earns its place now with something the earlier plan couldn't point to:
`DurableStore`, wired directly onto `kv.DurableRange`, so a transaction spanning two real
Raft groups is durable and crash-recoverable, not just correct against an in-memory
model.

## Context

The plan (`PLAN.md`, "Two decisions to make deliberately, not by inheritance") deferred
this exact question until sharding existed to make it concrete: does Phase 4's 2PC still
earn its place once the SQL layer that originally motivated it is gone? `kv.DurableRange`
answered the sharding half of that question in an earlier session. `internal/txn` itself
turned out to already be built -- HLC, `Store` (write intents, transaction records),
`Coordinator` (prepare/commit/resolve) -- with its own passing unit tests, but **entirely
unwired**: nothing outside the package imported it, the same "real code, zero real
callers" gap this project has already found and closed twice before (the register
torture workload, `harness/torture/workload/register.py`; and, still open,
`harness/torture/workload/vector.py`).

This session closed that gap for transactions specifically:

- `Coordinator` was refactored to depend on a `Participant` interface (`PutRecord`,
  `Record`, `WriteIntent`, `Resolve`) instead of the concrete in-memory `*Store`, so the
  exact 2PC protocol logic already proven in `txn_test.go` runs unmodified over a
  different backend -- no transaction logic is duplicated between the two
  implementations.
- `DurableStore` (`internal/txn/durable_store.go`) implements `Participant` by proposing
  JSON-encoded records/intents through any `rangeClient` (`kv.DurableRange` in
  production) instead of a Go map.
- Doing this surfaced a real design gap, not a cosmetic one: `Coordinator`'s logic
  assumes each `Participant` call is synchronously visible to the next one -- true for an
  in-memory map, false for `rangeClient.Put`, which only proposes and returns once
  locally appended (see `kv.DurableRange.Put`'s own doc comment). `DurableStore` closes
  this with `putAndConfirm`, which blocks until a proposed write reads back locally
  before returning, rather than changing `Coordinator`'s simpler synchronous contract.
- `TestCoordinatorCommitsAcrossRealRaftRanges` and `TestDurableStoreRecordSurvivesRestart`
  (`internal/txn/durable_store_test.go`) prove this against two and three real 3-node
  `kv.DurableRange` groups respectively -- a transaction committing across genuinely
  separate Raft groups, and a transaction record surviving a real node's restart, purely
  by replaying its own Raft log.

## Consequences

- `internal/txn` moves from "a correct model with no callers" to "a real, durable
  cross-range transaction primitive" -- the resume claim can now say a batch write
  spanning ranges is atomic and crash-safe, backed by an integration test against real
  Raft groups, not just the existing single-node-model unit tests.
- Not done, and deliberately out of scope for this pass: nothing in `cmd/consensa` or the
  gRPC server (`internal/server`) calls `Coordinator` yet -- `DurableStore` is proven
  reachable and correct, but no client-facing RPC drives it. `internal/kv.Router` also
  doesn't yet know how to route a multi-range transactional write to the right
  `DurableStore`s automatically; today's tests construct both participants by hand. Both
  are real next steps, not silently dropped.
- `WriteIntent`'s conflict check remains non-atomic with its own index update (documented
  in `DurableStore`'s doc comment) -- a real race under concurrent writers to the same
  key, acceptable for now because `kv.DurableRange` has no conditional/compare-and-swap
  Put to build a proper fix on, and closing it would mean adding that primitive first.
- A single-node Raft group cannot elect itself (`startPreVote` only messages *other*
  peers, so a lone node's own already-satisfied quorum is never reactively
  re-evaluated) -- found while writing this session's tests. Not fixed: production
  ranges are always ≥3 nodes, and special-casing single-node bootstrap in production code
  to serve a test convenience would be exactly the kind of complexity this project's own
  engineering rules argue against.
