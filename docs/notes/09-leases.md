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

**What is still not built, stated plainly:** there is still no closed-timestamp
advancement (nothing periodically proposes an updated `ClosedTimestamp` as the leader
keeps committing), and no actual follower-read RPC path serves a client from anything but
the leader -- `GrantLease`/`CurrentLease` close lease *replication* specifically, not the
whole read-path ladder. `Host.AppliedIndex()` still does not exist as a public accessor,
which closed-timestamp advancement would need to know how far a given replica has
actually caught up.
