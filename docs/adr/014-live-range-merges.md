# ADR 014: live range merges retain the left Raft group

## Context

Phase 12 required cold adjacent ranges to merge after load subsides. Splits already
create sibling ranges with the same replica placement and retire their parent. Recreating
that retired parent on merge would create a second, obsolete Raft history for a span that
already has two independently replicated histories.

## Decision

The left child survives. The right child is first frozen through a committed Raft command,
then its applied data is proposed into the left group. Once every source record is visible
there, the right child is retired with the existing `MarkRetired` /
`ErrRangeKeyMismatch` contract, and `Meta.Replace` replaces the two descriptors with the
left descriptor expanded through the right end.

The trigger requires both a combined-size floor and both per-range QPS values below a
cold floor. The first QPS sample after a split is ignored: it is necessarily zero and
would otherwise create a split/merge oscillation without observing real load.

## Consequences

This is a one-direction consolidation: it does not resurrect parents or move replicas.
It therefore applies only after sibling ranges share identical placement; a future
placement-changing merge would first need the already-existing membership movement path.
The barrier and retirement make stale routes fail rather than silently writing to the
absorbed range. They do not make independently cached metadata across separate processes
instantaneously consistent; callers retain the established refresh-and-retry behavior.

A write proposed just before `Freeze()` is observed locally can still commit just after
the freeze barrier in the shared Raft log; `apply()` then discards it as a no-op rather
than applying it, even though the client's `Propose` call may have returned success. This
is the existing "proposed is not committed" contract (see `AppliedCount`'s doc comment),
not a new hazard invented by merge -- callers already confirm visibility with a
read-until-visible retry (`putAndConfirm` / `insertAndConfirm`) rather than trusting
`Propose`'s return value alone, and that idiom is what makes a freeze-adjacent write safe
to lose silently instead of needing a distinct error path.

Merge eligibility today is tracked only for the sibling pairs a live split just created
(the `splitCompleted` bookkeeping in `main.go`), not by scanning the descriptor catalog
for any adjacent, cold, identically-placed pair. The two original static ranges (and the
vector plane's original range) can therefore never merge, even though they satisfy
`MergeDescriptors`' own adjacency check. Extending eligibility to the full catalog is
future work, not a correctness gap in what merge does once it decides to run.

`executeKVMergeIfRecommended`/`executeANNMergeIfRecommended` run unconditionally on every
process's own tick loop, using that process's own local child handle to call
`Freeze`/`ExecuteLiveMerge` -- there is no check that the local replica is actually the
current Raft leader of the left child before proposing the migration writes into it. A
propose against a non-leader replica fails, so on any process that isn't currently
leading the left child, the migration keeps retrying and failing every tick, and the
merge only succeeds on whichever process happens to hold that leadership. Because the
split and the merge attempt both run on whatever process led the *parent* range, and a
freshly created child range elects its own leader independently, that process is not
guaranteed to also lead the left child -- correctness is unaffected (a failed attempt is
a no-op, not a partial write), but the merge can sit retrying for as long as it takes the
existing leadership-affinity policy to converge the left child's leadership onto that
same process, rather than completing as soon as both ranges go cold. Confirmed directly:
running the merge demo showed exactly this pattern, the split's own process retrying and
failing for tens of seconds while the other two processes never attempted it at all.
