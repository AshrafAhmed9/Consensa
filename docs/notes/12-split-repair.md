# Phase 12: range split foundation

## Why does this exist?

Static ranges eventually create hot spots. A split must change ownership without letting a
key disappear or belong to both children.

## How does it work?

The parent descriptor is replaced atomically by two half-open child descriptors sharing a
boundary. Both inherit replicas, so no placement movement is needed for the base handoff.

## What alternatives existed?

Copying data before declaring metadata is safe but can leave routing unclear; exposing
children independently can create a gap. A metadata transaction defines one visible cut.

## What tradeoff was made?

This is the ownership primitive only. It intentionally avoids claiming live data movement,
hot-range policy, or HNSW repair until those are connected to Raft state.

**Update: live data movement across a real split is now proven, for the graph plane.**
`TestLiveSplitPreservesSearchCorrectness` (`internal/ann/durable_split_test.go`) builds
one real 3-node `DurableNode` group, inserts a two-cluster dataset, and splits it into two
fresh, independent real 3-node groups by re-proposing each vector into whichever child
owns it under the split predicate -- the plan's simplest documented strategy ("rebuild
both children from scratch"), not the cheaper incremental-repair or serve-stale-parent
options it also names. It proves three things against real Raft groups, not the existing
in-memory `HNSW.Split`/`Repair` unit test alone: no vector is lost or duplicated across
the split; each child's search results never leak data belonging to the other child; and
recall@5 against each child's own ground truth stays high (>= 0.80 in the test) after the
split -- the split does not silently degrade search quality for data that legitimately
belongs to that child.

**Update: the same proof now exists for the KV plane too.**
`TestLiveSplitPreservesKVCorrectness` (`internal/kv/durable_split_test.go`) is the direct
counterpart: one real 3-node `DurableRange` group is written across a chosen split key,
then split into two fresh 3-node child groups by reading the parent's full applied state
via `DurableRange.AllKeys()` and re-proposing each key/value into whichever child owns it
under `SplitDescriptor`'s new boundary. It proves the same three things `AllKeys` exists
for: no key lost or duplicated across the split; no cross-boundary key survives in either
child; and every migrated value is byte-identical to what the parent held, not just
present under the same key.

**Update: the trigger DECISION (not execution) is now real for the KV plane.**
`ShouldSplit(threshold, keys)` (`internal/kv/split.go`) decides whether a range's key
count has grown past threshold and, if so, returns the median key by sorted order --
chosen over a byte-value midpoint because the real key distribution can be arbitrarily
skewed, and the median of the keys actually present is what divides the real data close
to evenly. `DurableRange.MaybeSplitKey` wires it to a replica's own `AllKeys`.
`TestMaybeSplitKeyDrivesARealLiveSplit` proves the decision is genuinely usable by the
already-proven migration mechanism: a real 3-node group grows past threshold, the trigger
picks a real split key from that group's own applied data, and feeding it into the
identical `SplitDescriptor` + re-propose pipeline `TestLiveSplitPreservesKVCorrectness`
proves produces two correct, non-empty children.

**Update: routing metadata can now cut over live, for the KV plane.** The claim that
`Router`/`meta.go` "both operate on a fixed, static range set" turned out to be stale --
`Meta.Replace` and `Router.Refresh` already existed and were already tested against
descriptor math and the in-memory `MultiRaft` assembly
(`TestMetaReplacePublishesSplitAtomically`, `descriptor_test.go`), just never proven
against a split that actually moved real data through real Raft groups.
`TestLiveSplitUpdatesRoutingMetadata` (`internal/kv/durable_split_test.go`) closes that:
it performs the identical real migration `TestLiveSplitPreservesKVCorrectness` proves,
then publishes the new child descriptors via `Meta.Replace` and proves a fresh client
resolves correctly to the right child, and a client that cached the OLD parent
descriptor *before* the split -- `RoutedKV.retryRoute`'s own real production scenario --
correctly reroutes to the child once it refreshes.

**Update: `cmd/consensa` now checks the split-trigger decision on a real interval.**
`checkSplitRecommendations` runs `MaybeSplitKey` against both KV ranges every
`--split-check-interval` (default 5s) and exposes the result as a real Prometheus gauge,
`consensa_kv_split_recommended{range_id}` -- `TestCheckSplitRecommendationsSetsGaugeAboveThreshold`
proves it tracks a real 3-node group's real applied data. Deliberately its own goroutine,
not folded into the Raft tick loop the way closed-timestamp advancement was: `AllKeys`
never calls `Host.Propose`, so it can't reintroduce the same-mutex contention that made a
second Propose-calling goroutine unsafe there, and checking it off the tick loop entirely
keeps a large range's full-scan cost from ever delaying real-time Raft ticking.

**What this still does not prove, stated plainly:** this is the decision, checked and
observable, but there is still no automatic *execution* on either plane, and no QPS-based
trigger exists at all (size only). A live split still needs fresh child Raft groups stood
up at runtime -- new listeners, storage directories, IDs -- which is real, separate
orchestration work no running binary attempts. `cmd/consensa` does already assemble a real
`kv.Meta`/`kv.Router` for its two static ranges and passes it to `KVService`
(`main.go`) -- corrected here after an earlier version of this note claimed otherwise --
but nothing in the running binary ever calls `Meta.Replace` on it: the `Router` this test
proves cutover through and the one the real binary constructs are the same type, exercised
the same way, but nothing yet triggers a real split against a live deployment to exercise
that path end to end. There is still no *in-flight request* cutover (an RPC already routed
to the parent mid-split does not get redirected; a client must complete its current
attempt, then re-route on its next one) -- this project has never modeled that finer-grained
case. The keyspace descriptor split, the data split, and the metadata publish are each
proven independently but not yet wired to fire together as one atomic operation triggered
off real traffic. The "rebuild from scratch" strategy used by both planes is also, by the
plan's own account, the most expensive of the three named options: real production use
would want incremental repair or a stale-parent-during-rebuild fallback to avoid the
latency cliff this approach causes while every key or vector is re-inserted one at a time.

**Update: automatic split EXECUTION is now real, for the KV plane.**
`kv.ExecuteLiveSplit` (`internal/kv/execute_split.go`) factors the migration loop
`TestLiveSplitPreservesKVCorrectness` proved by hand into a reusable function: given an
already-running parent's applied data and two already-constructed child ranges, it
migrates every key via a confirmed Put/Get, retrying past "not leader" since only
whichever replica currently leads the new group can accept writes.
`cmd/consensa.executeSplitIfRecommended` wires it into the running binary: it stands up
two fresh child `kv.DurableRange` groups on the SAME process's already-shared
`MultiplexedTransport` (needing no new listener or address at all -- registering a new
range ID on an existing shared listener was already how the two original static ranges
themselves start), migrates data, then calls `Meta.Replace` and registers the new
children with the live `KVService`/`AdminService` so they are immediately reachable, not
just replicated. No cross-process coordination call decides *when* to split: every
process runs the identical check against identical Raft-replicated applied state, so all
three independently compute the same decision, split key, and deterministic child IDs
(`parentID*10+1`/`+2`) and each builds its own local replica of the same two groups --
the same handshake-free bootstrap the two original static ranges already use.
`TestConsensaBinaryExecutesALiveSplitAutomatically` proves it inside the real shipped
binary: real `TransactionalPut` writes cross a low `--split-threshold`, a new
`consensa_kv_split_executed_total` counter (not just the existing decision-only gauge)
confirms migration actually completed, and new writes spanning both halves of the
original keyspace keep succeeding afterward -- proving live traffic cutover, not just
silent data migration into orphaned ranges nothing can route to.

Three real bugs were found and fixed while wiring this, none by inspection -- each
caught by an actual CI failure, not a local hunch:
`AllKeys` previously filtered only Raft's own `"raft/"` reserved prefix, not
`internal/txn`'s equally-reserved `"txn/"` bookkeeping prefix (that package's own doc
comment already claimed this convention, but `AllKeys` never actually honored it) --
`ShouldSplit`'s median landed on a `txn/record/...` key outside the parent's own interval
and every migration attempt failed with `"kv: split key outside parent interior"`.
Separately, a transient migration failure was re-triggering `newChild` on the very same
range IDs on every retry tick, opening a second `storage.Engine` against a directory the
first attempt's `Host` was still actively using -- corrupting the WAL and crashing the
whole process via `newKVRange`'s own `fatal()`. `executeSplitIfRecommended` now caches
the two child ranges across retries of the same parent, and `AllKeys` excludes both
reserved prefixes.

Third, and the one that took real CI evidence to see: `kv.ExecuteLiveSplit`'s own
`putAndConfirm` only checked whether a key had become visible (`Get`) inside the branch
where this process's own `Put` had just succeeded -- so a process whose local replica
never won the new child group's leadership (2 of the 3, always) kept retrying `Put`
until `perKeyTimeout` on every single key, never once checking whether the actual leader
(a different process) had already committed and replicated the value to it. CI showed
this precisely: one process logged "live split executed" within a second of the decision
firing, while the other two spent the entire test deadline retrying "not leader" on data
that had already arrived locally via ordinary replication. `putAndConfirm` now checks
`Get` on every iteration regardless of whether `Put` itself succeeded this round.

**What this still does not prove, stated plainly:** transaction bookkeeping keys
(`txn/...`) are still deliberately excluded from migration, not moved with the data they
describe -- originally justified on the grounds that the parent range is kept around
rather than deleted, so any in-flight transaction's own bookkeeping remains locally
readable there. **Update: that justification is now narrower than it reads above.**
The parent range is still kept around, not deleted, but it no longer accepts *any*
request once retired -- `DurableRange.MarkRetired()` (called right after migration, right
before the new routing is published) makes `Get` return `ErrRangeKeyMismatch` on the
parent just like `Put`/`Delete`, for every key including `txn/...` bookkeeping. A
transaction whose bookkeeping still lives on a parent that has since retired loses read
access to it too, same as any other post-split reader; this project still has not
modeled that edge case further. What *is* now fixed is the bigger problem: before this,
an RPC already routed to the parent mid-split, or one arriving in the brief window before
the new routing was published, would silently succeed against data that had already
moved to a child, with no error and no signal that anything was wrong. Now `MarkRetired()`
is called before `meta.Replace` publishes the new routing, so the parent stops accepting
requests before a client could even be told to stop sending them there; the caller gets
`ErrRangeKeyMismatch` and retries through the existing stale-route-refresh contract,
rather than reading or writing state that has already diverged from the children. See
`docs/adr/013-parent-range-retirement.md`. This does not claim a zero-window guarantee
across independently-updated process-local views of routing -- only that the parent
itself can no longer be the source of silent divergence. The "rebuild from scratch" strategy remains the most expensive of the
three named options -- real production use would want incremental repair or a
stale-parent-during-rebuild fallback to avoid the latency cliff every key pays while
being re-inserted one at a time.

**Update: automatic split EXECUTION now also exists for the vector plane, closing the
one remaining gap named just above.** `ann.ExecuteLiveSplit` (`internal/ann/execute_split.go`)
is the vector-plane counterpart of `kv.ExecuteLiveSplit`: same "rebuild from scratch"
strategy, migrating every applied vector into whichever fresh child owns it via a
confirmed Insert/GetVector loop. It deliberately does NOT reuse the pre-existing
`HNSW.Split` (`split.go`), which clones and repairs a graph already held in one process's
own memory -- a live split spanning real, independently-running processes has to go
through Raft the same way any other mutation does, which `HNSW.Split` was never built to
do. `ann.ShouldSplit` provides the decision (median vector ID by sorted order -- the same
minimum-viable, honestly-labeled limitation `kv.ShouldSplit` has: a lexicographic ID
bisection, not a clustering-aware vector-space boundary, so recall near the boundary can
dip until each child's own graph structure compensates. This is deliberately NOT PLAN.md's
Phase 10 answer to HNSW-under-splits, which calls for a dedicated ADR measuring rebuild vs.
incremental-repair vs. stale-parent strategies -- this is the minimum viable decision
proving automatic execution works end-to-end for this plane too, postponing that measured
work rather than inventing an unproven heuristic in its place).

Unlike the KV plane's two static ranges, the vector plane previously had no routing layer
at all -- `server.Service` held a single `index Index` field with no per-range map. It now
holds `meta *ann.Meta` (mirroring `kv.Meta`/`kv.Router` exactly) plus `indices
map[uint64]Index`, with `Upsert`/`Delete`/`BatchGet` routing by ID through `meta.Lookup`
and `RegisterIndex` (the counterpart of `KVService.RegisterStore`) wiring in a live
split's fresh children. `Search` has no ID to route by, so it fans out to every currently-
registered range and merges each one's own top-k candidates by distance -- the standard
scatter-gather shape a sharded ANN index needs once data can legitimately live in more
than one range. `cmd/consensa.executeAnnSplitIfRecommended` wires the whole thing into the
running binary exactly like its KV counterpart, using child range IDs `parentID*100+1/+2`
(not KV's `*10`) so the two planes' transport-multiplexed range IDs, sharing the same
`MultiplexedTransport`, can never collide -- both planes' parent IDs happen to be `1`,
so `*10` would otherwise produce `11`/`12` for two entirely different Raft groups.
`TestConsensaBinaryExecutesALiveVectorSplitAutomatically` proves it inside the real
shipped binary, the vector-plane counterpart of the KV proof above.

Two more real bugs were found and fixed chasing this, both by an apparent test hang, not
by inspection. First: `HNSW.Insert`'s own doc comment claimed "adds or replaces an
embedding," but the code actually returned an error for a duplicate ID instead of
replacing. `ExecuteLiveSplit`'s own `insertAndConfirm` legitimately retries `Insert` for
the same ID until it observes the value via `GetVector` (a prior attempt may already be
committed but not yet visible -- see `kv.putAndConfirm`'s identical reasoning), and when
a second proposal for an already-applied ID eventually committed too, `ApplyMutation`'s
resulting error propagated out of `Host.driveLocked` *before* it reached `Node.Advance()`
-- meaning that replica's Raft loop could never clear the entry and re-emitted the exact
same committed entry, and every message alongside it, on every subsequent tick, forever.
This is a real, general Raft-correctness principle, not an ann-specific quirk: an Apply
callback must never fail for an entry that already achieved consensus, since there is no
way to "reject" something the log already committed. Fixed by making `HNSW.Insert`
actually implement the "replace" semantics its own doc comment already promised (delete
the old node via `Delete`'s existing `Repair`-based cleanup, then insert fresh).

Second, and only findable once the first was fixed: `internal/raft/tcp.go`'s
`TCPTransport.Send` dialed with a 1-second timeout but never set a write deadline on the
resulting connection -- a peer whose own receive loop stalls (for any reason, including
the first bug above) could block a write indefinitely, and since `Host.driveLocked` calls
`Send` while holding the host's own mutex, that indefinite block held the lock forever
too. Fixed by setting a matching 1-second deadline on the connection before writing.

**Update: the split trigger is no longer size-only.** `kv.ShouldSplit`/`ann.ShouldSplit`
now take a `SplitTrigger{SizeThreshold, QPS, QPSThreshold}` instead of a bare `int`
threshold, and recommend a split once EITHER active (positive) criterion is exceeded --
size alone can never catch a range that is small by key/vector count but genuinely hot
under a skewed access pattern (a handful of keys or vectors accessed far more often than
the rest of the range), which is exactly PLAN.md's own named gap this closes ("no
QPS-based trigger exists, only size").

Getting a real QPS number required a real per-range request counter, which did not exist
before: `kv.DurableRange`/`ann.DurableNode` each gained their own `requestCount
atomic.Uint64`, incremented on every `Put`/`Delete`/`Get` or `Insert`/`Delete`/`Search`
respectively -- the same shape and reasoning as `server.Service`'s own pre-existing
`requestCount`, but per-replica instead of per-process, since a split decision needs to
know THIS range's load, not the whole node's aggregate. `cmd/consensa`'s new `qpsTracker`
turns that raw counter into a rate via the identical delta-over-a-window technique the
pre-existing `consensa_range_qps` sampling loop already used for the whole node, just
applied per range ID instead of once globally -- computed exactly once per range per
split-check tick and reused for both the recommendation gauge and the execution call, since
`qpsTracker.rate` is a one-shot sample that would silently corrupt its own measured window
if called twice against the same baseline in the same tick.

`--split-qps-threshold` (default `0`, disabled) controls this independently of
`--split-threshold`, so a deployment that never sets it keeps today's size-only behavior
exactly. The split-point choice itself is unchanged regardless of which criterion fired --
still the median key/ID by sorted order, since the goal either way is two roughly-equal
children, not a boundary that reflects WHY the split triggered.

**What this still does not prove, stated plainly:** the QPS measured is per-replica, not
cluster-wide -- a range whose write traffic is served by one leader but whose read traffic
is spread across followers (this project's own follower-read support, `docs/notes/09-leases.md`)
would undercount its true aggregate load unless every replica happens to run with the same
`--split-qps-threshold` and independently reaches the same conclusion, which the existing
"every process runs the identical check" design already relies on for the size trigger too.
There is no smoothing or hysteresis on the QPS signal -- a single hot tick can recommend a
split that a slightly later, quieter tick would not have, though `executed`/`inProgress`'s
existing once-per-parent guard means a recommendation only ever triggers execution once,
not a flapping series of attempts.

## What can fail?

The metadata replacement must be a transaction in the real cluster. HNSW cross-boundary
edges must be repaired before child graphs serve approximate search independently.

The reciprocal merge primitive accepts only adjacent spans with identical replicas. A live
merge still needs a coordinated Raft barrier before it can publish that descriptor change.
