# Phase 9: leases and follower reads

## Why does this exist?

Leader-only reads turn every lookup into a single-replica bottleneck. A closed timestamp
lets caught-up followers safely serve reads that are explicitly old enough.

## How does it work?

A lease names an authorized holder and validity interval. A follower read additionally
requires the local applied index to reach the closed timestamp’s index and the requested
timestamp not to exceed its promise.

The leader fallback is now a conservative `ReadIndex` barrier: it commits a reserved Raft
no-op and waits for local application. That commit is a quorum proof with no clock
assumption, so an isolated former leader times out instead of returning a falsely
linearizable value. It costs a log entry per protected read for now; heartbeat-context
ReadIndex is the later optimization, not a different safety contract.

## What alternatives existed?

ReadIndex quorum confirmation is stronger without clock assumptions, but currently costs
a quorum round trip and a no-op log entry on every read. Leases make the clock assumption
explicit.

## What tradeoff was made?

Only bounded-staleness reads are authorized. This avoids claiming linearizability for a
follower response that has not performed a leader round trip.

## What can fail?

Clock skew beyond the configured bound invalidates the lease argument. The production
assembly must revoke leases conservatively on uncertainty or leader changes.

ADR-009 records the exact `max_offset` assumption and requires `ReadIndex` as the fallback
whenever a node cannot establish that its lease remains safe.

## Status

**`FollowerReadAllowed` and `ClosedTimestamp` (`internal/kv/lease.go`) are a correct,
tested model with zero real callers** -- the same gap this project has now found and
closed for the torture harness's register and vector workloads and for
`internal/txn`. Fully wiring it needs a `Host.AppliedIndex()` accessor (does not exist
yet -- `raft.Node`'s applied index is private to the log) and a real leader-side
closed-timestamp advancement-and-propagation loop, which is genuinely this phase's
~2-week scope, not a quick patch.

**A narrower, more urgent gap was found and fixed instead: `DurableRange.Get()`'s own
doc comment overclaimed linearizability.** It read "any replica -- not only the leader --
may safely serve reads," reasoning by analogy to `DurableNode.Search` (correct there,
since ANN search is already documented as approximate/bounded-staleness by design). For
the KV plane, though, `PLAN.md`'s own Claims Discipline section claims the KV/metadata
plane specifically *is* linearizable -- and a `Get()` routed to a replica that has fallen
behind can return a value staler than one a client already observed acknowledged
elsewhere, which is a real linearizability violation, not a cosmetic one. `Get()`'s doc
comment now says plainly what it actually provides (bounded staleness, not
linearizability), and a new `ConsistentGet` requires this replica to currently be leader
(mirroring `Put`/`Delete`'s "not leader" contract) before reading -- since a leader has,
by definition, applied everything it has itself committed, a read against the same
leader that acknowledged an earlier write is guaranteed to observe it.
`TestConsistentGetRequiresLeadership` (`internal/kv/durable_range_test.go`) proves both
halves: a follower rejects it, the leader serves the value it just committed.

**Update: lease grants are now real, Raft-replicated state, not just a tested model.**
`DurableRange.GrantLease` proposes a lease-grant command through the same
Raft-replicated wire format `Put`/`Delete` use (`multiraft.go`'s `rangeCommand`, extended
with `LeaseHolder`/`LeaseStart`/`LeaseExpiration`), so a grant only takes effect once
every replica has actually applied it, not merely accepted locally on the leader.
`DurableRange.CurrentLease()` exposes each replica's own applied result.
`TestGrantLeaseReplicatesToEveryReplica` proves convergence across a real 3-node group
and that `FollowerReadAllowedWithOffset` correctly accepts a read inside the lease
interval and rejects one after expiry, against the replicated lease rather than the
in-memory model alone.

**Update: the whole ladder now closes end to end at the `DurableRange` layer.**
`AdvanceClosedTimestamp` proposes a `commandClosedTimestamp` through the identical
Raft-replicated wire format -- each replica pairs the leader-fixed `Timestamp` with its
own `entry.Index` at apply time, since how far *this* replica has applied is inherently
per-replica even though the promise itself is not. `DurableRange.AppliedIndex()` closes
the `Host.AppliedIndex()` gap directly on `DurableRange` (tracked via the existing
`Apply` callback) rather than adding it to `raft.Host`'s own surface -- no change to
`Host` was needed. `DurableRange.FollowerRead(key, readAt, maxOffset)` ties all of it
together: it serves `key` from local storage only when
`FollowerReadAllowedWithOffset` says a valid, *replicated* lease is held by this exact
replica, this replica's applied index has caught up to the closed-timestamp promise, and
`readAt` doesn't exceed it.

`TestFollowerReadServesOnceLeasedAndClosed` proves this against a real 3-node group,
checking every rejection path, not just the success path: `FollowerRead` fails with no
lease; still fails with a lease but no closed timestamp; succeeds once both have
replicated and applied; and fails again on a replica that is not the lease's intended
holder (the leader itself) even though the identical closed timestamp has applied there
too -- proving the check is genuinely per-holder, not "any caught-up replica."

**Update: `cmd/consensa` now advances closed timestamps and grants/renews leases on a
real interval.** `advanceClosedTimestamps` and the newer `maintainLeases`
(`cmd/consensa/main.go`) both run inside the same tick-count-gated goroutine as Raft
ticking itself -- deliberately the same goroutine, not two: `GrantLease` and
`AdvanceClosedTimestamp` both call `Host.Propose`, and a second goroutine independently
calling `Propose` against the same `*raft.Host` doubles contention on the mutex
`driveLocked` holds across a blocking network send (found as a real regression while
wiring closed-timestamp advancement -- see that fix's own commit). `maintainLeases`'s
policy: whichever replica is currently Raft leader grants itself a lease once its
current one is not valid comfortably past `--lease-renew-before`, so a valid lease
exists continuously without re-proposing one that's still fine.
`TestMaintainLeasesGrantsAndRenewsOnlyOnLeader` proves both halves against a real 3-node
group: the lease is granted and applied, and a second call while it's still comfortably
valid produces no second proposal.

**What is still not built, stated plainly:** no lease revocation or handoff on
leadership change (a lease granted by a leader that then loses leadership is not
actively invalidated -- it simply expires on its own clock, and the new leader's own
`maintainLeases` call grants a fresh one once it notices). No RPC surface exposes
`FollowerRead` to a network client the way `ConsensaKV.TransactionalPut` exposes 2PC;
this closes the mechanism and the automatic policy, not a client-facing API on top of
it.
