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

## What can fail?

The metadata replacement must be a transaction in the real cluster. HNSW cross-boundary
edges must be repaired before child graphs serve approximate search independently.

The reciprocal merge primitive accepts only adjacent spans with identical replicas. A live
merge still needs a coordinated Raft barrier before it can publish that descriptor change.
