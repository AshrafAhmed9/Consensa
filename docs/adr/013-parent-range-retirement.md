# ADR 013: parent range retirement on live split

## Context

`docs/notes/12-split-repair.md` named a stated simplification when live split execution
first shipped: the parent range is deliberately kept running after a split completes,
rather than deleted, so that anything still referencing it (notably in-flight
transaction bookkeeping under `txn/...`, which is excluded from migration) keeps working.

That simplification had a real, previously-unclosed gap: "kept running" meant the parent
kept accepting ordinary `Put`/`Get`/`Delete` (KV) and `Insert`/`Search`/`GetVector`
(vector) requests indefinitely, for keys and vectors that had already migrated to child
ranges. A request that raced the split, or arrived in the window between migration
finishing and the new routing being published, would succeed against the parent instead
of erroring -- silently diverging from whatever the children now held. Nothing detected
or rejected this; it just accumulated as stale state on a range nothing was supposed to
be routing to anymore.

## Decision

**The parent range now refuses every request once its data has migrated, instead of
continuing to serve it.**

- `internal/kv/durable_range.go`: `DurableRange` gained a `retired atomic.Bool` field,
  `MarkRetired()`, and `Retired() bool`. `Put`, `Delete`, and `Get` all check it first and
  return `kv.ErrRangeKeyMismatch` once set.
- `internal/ann/durable.go`: `DurableNode` gained the identical pattern. `Insert`,
  `Delete`, and `Search` return `ann.ErrRangeKeyMismatch`; `GetVector` (which has no error
  return, its signature is `(vector.Vector, bool)`) returns `(nil, false)`.
- `cmd/consensa/main.go`: `executeSplitIfRecommended` and `executeAnnSplitIfRecommended`
  both call `parent.MarkRetired()` immediately after migration succeeds, and *before*
  `meta.Replace` publishes the new routing. Ordering matters here: retiring first means
  the parent stops accepting requests before any client could even learn to stop sending
  them there. Retiring after would leave the same window this ADR closes.
- `ErrRangeKeyMismatch` is not a new failure mode for callers -- it is the same sentinel
  `RoutedKV` already treats as "refresh metadata and retry" elsewhere in this codebase.
  A write already in flight against the parent, or one landing in the brief gap before
  routing updates, now fails cleanly and retries through that existing contract instead
  of silently succeeding against data that has already moved.

## Consequences

This closes the silent-divergence gap: a retired parent can no longer accept a write or
serve a read for data it no longer owns. It does not claim a zero-window guarantee
between independently-updated processes' local views of routing -- a client could in
principle still observe a brief interval where its cached routing points at a
now-retired parent, but the parent itself will reject it rather than accept it, so the
failure mode is a clean retry, not silent divergence.

One consequence worth stating plainly: retirement also blocks reads, not just writes.
The original justification for keeping `txn/...` bookkeeping keys off migration was that
the parent stays around and remains locally readable. That is no longer fully true once
a parent retires -- `Get` on a retired parent now fails for every key, including
transaction bookkeeping, the same as any other post-split read. This project has not
modeled the edge case of a transaction still in flight at the exact moment its
bookkeeping range retires; it is a narrower version of the same edge case
`docs/notes/12-split-repair.md` already flagged as unmodeled, not a new one introduced
here.
