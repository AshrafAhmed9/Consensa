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

**What this does not prove, stated plainly:** there is still no automatic trigger (size or
QPS threshold) on either plane, no live traffic cutover (both tests build fresh groups and
read/search them directly; nothing routes an in-flight client from the parent to the
correct child mid-split), and the keyspace descriptor split and the data split are each
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
