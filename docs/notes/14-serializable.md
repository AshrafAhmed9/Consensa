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
- `DurableStore`, the Raft-backed `Participant` used in production, does not implement
  this check yet -- the protection currently exists only in the in-memory `Store` model
  the 2PC protocol logic is proven against, not yet in the durable, replicated path. A
  real per-range, durable `TimestampCache` (surviving restart, per this file's own "what
  can fail" section above) is separate work.
- No read-refresh: the classic full SSI/CockroachDB design lets a pushed transaction
  re-validate its own prior reads at the new timestamp and continue if nothing changed,
  rather than aborting outright. That refinement is not implemented -- this closes
  detection and rejection, not the friendlier retry-without-full-abort path.
