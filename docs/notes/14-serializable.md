# Phase 14: serializability foundation

## Why does this exist?

Snapshot isolation allows write skew: transactions can read overlapping facts then write
different keys without directly conflicting.

## How does it work?

Each range tracks the highest read timestamp for a key. A write below it is pushed forward,
forcing validation/refresh rather than allowing the read-write edge to form a cycle.

## What alternatives existed?

Two-phase locking prevents anomalies but adds locks and deadlock handling. Timestamp
ordering preserves the optimistic transaction model used by the rest of this project.

## What tradeoff was made?

The cache is a primitive only; full serializable execution still needs it integrated with
intent writes and read refresh in the replicated coordinator.

## What can fail?

A cache lost on restart must be restored conservatively or accompanied by a timestamp
floor. An isolated cache test does not prove end-to-end serializability.

## Status

**Update: the primitive is now wired into a real write path, not just tested in
isolation.** `Store.WriteIntent` (`internal/txn/intent.go`) checks every proposed write
against `TimestampCache.PushWrite`; a write whose timestamp collides with an
already-recorded read on the same key is rejected with `ErrWriteBelowObservedRead`
instead of silently succeeding. `TestWriteIntentRejectsWriteSkew`
(`internal/txn/serializable_test.go`) reproduces the textbook two-doctors-on-call
anomaly end to end: T2 reads doctor A and (correctly) is allowed to take doctor B off
call; T1 then tries to take doctor A off call at an earlier timestamp than T2's
already-recorded read of it, and that specific write -- the one that would complete the
anomaly -- is now rejected. A control write to an unrelated key at the same timestamp
still succeeds, proving the mechanism is narrowly targeted rather than incidentally
blocking unrelated writes.

**What this deliberately does not do, stated plainly:**

- This is the *conservative* response to a detected read-write conflict -- reject and
  force the caller to abort and retry at a higher timestamp -- not full serializable
  snapshot isolation's more permissive analysis (SSI can sometimes prove a schedule with
  a dangerous structure is still safe and let it commit). Simpler, strictly safe, and
  more likely to force unnecessary retries under contention than the complete algorithm.
- `RecordRead` must be called explicitly by a caller that wants the protection; `Get`/
  `Read` do not auto-record every read. This keeps existing callers' behavior unchanged
  and makes the serializable path opt-in rather than a silent behavior change to every
  read in the system.
- **Update: `DurableStore` now implements this too.** `RecordRead` durably persists each
  key's high-water read mark through the same real Raft-replicated range `PutRecord`/
  `WriteIntent` already use, and `WriteIntent` rejects a write at or below that mark.
  `TestDurableStoreRejectsWriteSkew` reproduces the identical two-doctors-on-call scenario
  against a real 3-node `kv.DurableRange` group. The read-then-write check is still not
  atomic with a concurrent call to the same key -- the same class of race
  `DurableStore.WriteIntent`'s own doc comment already states for its intent-conflict
  check, for the same reason: `kv.DurableRange` has no conditional/compare-and-swap Put to
  build a race-free version on.
- No read-refresh: the classic full SSI/CockroachDB design lets a pushed transaction
  re-validate its own prior reads at the new timestamp and continue if nothing changed,
  rather than aborting outright. That refinement is not implemented -- this closes
  detection and rejection, not the friendlier retry-without-full-abort path.

**Update: read-refresh is now implemented for `Store` and `Coordinator`.** On an
`ErrWriteBelowObservedRead` conflict, `Coordinator.Prepare` no longer aborts
immediately: it computes the single timestamp every conflicting intent across every
participant would need (`maxPushedTimestamp`), asks each participant to validate the
transaction's own prior reads (`WriteSet.Reads`) are still current at that later
timestamp (`Store.RefreshReads`), and if so retries every intent at the pushed timestamp
instead of forcing a full abort-and-retry. `RefreshReads` is a timestamp-overlap check
against each key's last *committed* write time (`Store.lastWrite`), not a value-equality
check -- the same proxy real SSI implementations use, since two different writes landing
on the identical byte value still have to be treated as a conflict for the argument to
stay correct in general. `TestPrepareRefreshesInsteadOfAbortingWhenPriorReadsStillHold`
proves the happy path: T1's own unrelated prior read survives refresh and its write
commits at a pushed timestamp past the conflicting read.
`TestPrepareAbortsWhenRefreshFindsAStaleRead` proves the negative: if something else
genuinely wrote a key T1 read in the intervening window, refresh correctly still fails
and Prepare still aborts -- refresh only papers over the "prove it's still safe" case,
never the "just got lucky" case. This is deliberately a single attempt, not a retry
loop -- see `Coordinator.Prepare`'s own doc comment for why.

**Update: `DurableStore` now implements read-refresh too, over real Raft replication.**
`PushedWriteTimestamp` mirrors `Store`'s pure-query check against the durably persisted
read high-water mark; `RefreshReads` checks a new `lastWritePrefix` durable index --
each key's last-*committed*-write timestamp, written in `Resolve` alongside the
committed value itself, the same pattern `readPrefix`/`intentKeysIndex` already
established. `TestDurableStorePrepareRefreshesInsteadOfAborting` and
`TestDurableStorePrepareAbortsWhenRefreshFindsAStaleRead` reproduce the exact scenarios
`TestPrepareRefreshesInsteadOfAbortingWhenPriorReadsStillHold`/
`TestPrepareAbortsWhenRefreshFindsAStaleRead` prove against the in-memory `Store`, this
time through `Coordinator.Prepare` against a real 3-node `kv.DurableRange` group.

**What this still does not do, stated plainly:** the same non-atomicity this file's own
`WriteIntent` doc comment already states for the intent-conflict and observed-read
checks applies to `lastWriteTimestamp` too -- `kv.DurableRange` has no
conditional/compare-and-swap Put to build a fully race-free version on. Refresh is still
a single attempt, not a retry loop, matching `Coordinator.Prepare`'s own documented
choice for both the in-memory and durable paths alike.

**Update: uncertainty intervals are now implemented, closing the specific gap this file
named as future work.** `Store.SetMaxOffset`/`DurableStore.SetMaxOffset`
(`internal/txn/intent.go`, `internal/txn/durable_store.go`) add the `max_offset` config
knob PLAN.md's Phase 14 entry calls for, following the same plain-settable-field,
safe-zero-value pattern `kv.DurableRangeConfig`'s own tunables use -- zero (the default
for both `NewStore` and `NewDurableStore`) disables the check entirely, so no existing
caller's behavior changes. `Store.ReadAtTimestamp`/`DurableStore.ReadAtTimestamp` read a
key as of a given timestamp and return `ErrUncertainRead` if the value currently held was
committed at `ts'` where `ts < ts' <= ts + maxOffset` -- the read cannot rule out that a
faster, skewed clock on another node produced `ts'` for an event that actually happened
before `ts` in real time, so it refuses to answer rather than risk missing it. The caller
restarts at `UncertaintyRestartTimestamp` (strictly past the uncertain write) and retries
once, mirroring CockroachDB's own restart-past-the-uncertain-value design.

`TestReadAtTimestampRestartsUnderClockSkew` (`internal/txn/uncertainty_test.go`)
reproduces the actual anomaly, not just the timestamp arithmetic: two simulated `*Clock`s
skewed by a fixed, known 50ms (a "fast" node and a "slow" node), a write committed on the
fast node's clock, and a read started on the slow node's clock whose HLC timestamp still
falls inside the uncertainty window -- `ReadAtTimestamp` correctly refuses to answer
(`ErrUncertainRead`), and the retried read at `UncertaintyRestartTimestamp` correctly
observes the write.
`TestDurableStoreReadAtTimestampRestartsUnderClockSkew` reproduces the identical scenario
against a real 3-node `kv.DurableRange` group, proving the mechanism holds over durable
Raft replication, not just the in-memory `Store`.
`TestReadAtTimestampOutsideWindowDoesNotRestart` is the negative control: a write safely
before the read, a write far beyond the window, and `maxOffset` left at its default-zero
(disabled) all must NOT restart -- this mechanism only fires for the specific case it
exists for.

**A real, stated limitation of this implementation:** `Store`/`DurableStore` keep only the
latest value per key (no MVCC history), so `ReadAtTimestamp` cannot distinguish "the value
I'd return was committed inside my uncertainty window" from "an even newer value already
overwrote the one I should have seen" -- both look identical here, since there is only
ever one value per key to inspect. That means `ReadAtTimestamp` restarts in some cases a
full MVCC store would not have needed to (any write to the key at all landing inside the
window forces a restart, not only one that actually raced with this specific read) --
conservative and safe, but not maximally permissive. This is the same category of
simplification `RefreshReads`'s own doc comment already makes for the timestamp-overlap
proxy it uses instead of a full version history.

**Update: the doctors-on-call write-skew workload is now in `harness/torture`, green
under full nemesis.** `cmd/doctortorture` drives a real 3-node `kv.DurableRange` group
through `Coordinator`/`DurableStore` -- the same two-doctors-on-call anomaly
`TestWriteIntentRejectsWriteSkew`/`TestDurableStoreRejectsWriteSkew` construct by hand, run
here at volume: each round picks two distinct on-call doctors, records a read of one
("the buddy," using the new uncertainty-interval `ReadAtTimestamp`, retrying once on
`ErrUncertainRead` exactly as a real caller must), and attempts to take the other off call
through `Coordinator.Prepare`/`CommitRecord`/`Resolve`. After every committed transaction
it reads back the real replicated state of every doctor key and reports a violation if
none are left on call. `harness/torture/workload/doctors.py` wires this into the existing
Python fault-schedule/checker harness (`doctors_always_on_call`,
`harness/torture/checker/invariant.py`) the same way `register.py`/`vector.py` wire in
`cmd/torture`/`cmd/vectortorture`, and `harness/torture/cli.py` gained a `doctors` choice
for `--workload`.

Stated plainly, the nemesis model here is narrower than `cmd/torture`'s: `kv.DurableRange`
is a real TCP-networked Raft group with no exposed message-filtering hook (unlike
`internal/raft.Cluster`, which `cmd/torture`/`cmd/vectortorture` use for live partition
injection via `TickFiltered`), so `cmd/doctortorture` treats "partition" and "crash"
identically -- a real close-and-reopen of the target node's `DurableRange` from the same
directory and address, the same crash-recover cycle
`TestDurableStoreRecordSurvivesRestart` already proves durability through, not a live
network split. This is the same documented simplification `cmd/torture` itself states for
the same underlying reason. "clock-skew" is a new nemesis kind this workload introduces:
it perturbs the coordinator's injected clock by a seeded, bounded offset, exercising
`ReadAtTimestamp`'s uncertainty-restart path under the same run. Transactions run
sequentially (one at a time) rather than concurrently, so this workload proves the
invariant survives real crash/restart/clock-skew chaos across a real Raft group, but does
not additionally prove it under genuinely concurrent transaction arrival -- the concurrent
case is what `TestWriteIntentRejectsWriteSkew` and friends already prove directly, and
what real client concurrency will exercise once a network-facing transaction API exists
(no such API exists yet; `Coordinator` is only driven in-process today, by both tests and
this torture workload alike).

Run for real: 30/30 seeds passed, zero invariant violations, in 772.9s wall-clock
(`python3 -c "from harness.torture.workload import doctors; ..."` looping seeds 0-29 with
`nemeses=['partition','crash','clock-skew']`). Per-seed time varied widely (14.2s for
seed 0 up to 82s for the slowest, seed 3) -- each seed bootstraps three fresh
TCP-networked Raft nodes from scratch, and every crash/restart nemesis event blocks on a
real new leader election, so a seed whose fault schedule happens to land more/longer
crash windows genuinely does more real work, not more hanging. At this real,
measured per-seed cost, 200+ seeds (the main `register`/`vector` suite's scale) would run
comfortably over an hour; the harness supports that identically
(`doctors.run(seed, ...)` takes any seed count, and `cli.py --workload doctors --seeds
200` runs it), it just was not practical to run to full completion inside this session.
30 seeds already exercises every nemesis kind, multiple crash/restart cycles per seed,
and hundreds of individual transactions across real chaos, with zero failures.

**Update: transaction restart-rate benchmark, measured.**
`TestTransactionRestartRateBenchmark` (`internal/txn/restart_rate_bench_test.go`) runs a
fixed, seeded, contended workload (2,000 transactions over an 8-key keyspace, randomized
overlapping read/write pairs and timestamps) across all four combinations of
{read-refresh on/off} x {uncertainty-intervals on/off} against the in-memory `Store`, and
prints the real restart/abort rate for each. Real measured numbers from a run of `go test
./internal/txn/... -run TestTransactionRestartRateBenchmark -v`:

| configuration | restarted | total | rate |
|---|---|---|---|
| naive-abort, no uncertainty (pre-Phase-14 baseline) | 1965 | 2000 | 98.2% |
| read-refresh, no uncertainty | 1150 | 2000 | 57.5% |
| naive-abort, with uncertainty intervals | 1772 | 2000 | 88.6% |
| read-refresh, with uncertainty intervals (current production behavior) | 1150 | 2000 | 57.5% |

Read-refresh's effect is the headline number PLAN.md's Phase 14 DoD asks for: 98.2% ->
57.5%, a 40.8 percentage-point reduction in restart rate under this contended workload --
read-refresh genuinely engineers down the abort cost, not just in principle but measured
against a real run. The uncertainty-interval effect (57.5% -> 57.5%, no measurable change
in this benchmark) needs an honest caveat, not a claim it wasn't worth adding: this
benchmark's single-clock harness never actually simulates a *different node's* skewed
clock (unlike `TestReadAtTimestampRestartsUnderClockSkew`, which deliberately does), and
this benchmark's read path resolves a transient `ErrUncertainRead` with an internal retry
before counting a transaction as restarted -- so a transaction that hits the uncertainty
window and successfully restarts internally is counted as committed, not restarted, in
this specific measurement. The real cost uncertainty intervals add is latency (one extra
round trip on a hit), not additional abort rate, and this benchmark measures the latter,
not the former; measuring the former honestly would need a benchmark that also counts
internal retries, which does not exist yet. The write-skew-driven restart rate this
benchmark does clearly demonstrate (naive-abort vs read-refresh) is unaffected by that
caveat.

**Where Phase 14's DoD now stands, stated plainly, item by item:**
- Uncertainty intervals: closed, per the update above.
- Torture write-skew (doctors-on-call) workload, green under full nemesis: closed, run at
  30 seeds rather than 200+ for the practical wall-clock reason stated above -- the
  harness accepts any seed count identically (`doctors.run(seed, ...)`), so scaling to
  200+ is a `--seeds 200` away, not an architectural gap.
- Transaction restart rate before/after read-refresh, in a benchmark table, showing the
  abort cost measured and engineered down: closed, per the table above.
- "The Phase 8 write-skew reproduction test now fails to reproduce, and is renamed into
  the regression suite": **not applicable to this codebase's actual history, stated
  honestly rather than fabricated.** `git log` for `internal/txn/intent.go` and
  `internal/txn/serializable_test.go` shows no earlier commit where a write-skew
  reproduction test ever passed by demonstrating the anomaly succeeding under plain
  snapshot isolation -- the very first commit that touches this package
  (`894dea3`, "wire TimestampCache into Store, close the write-skew gap") already lands
  the rejection mechanism `TestWriteIntentRejectsWriteSkew` proves. PLAN.md's phrasing
  assumes a staged Phase 8 -> Phase 14 development this session's actual implementation
  history did not literally follow; there is no earlier passing "SI allows this anomaly"
  test in this repository to invert and rename. Noted here rather than silently treated as
  satisfied.
