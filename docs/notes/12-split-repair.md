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

## What can fail?

The metadata replacement must be a transaction in the real cluster. HNSW cross-boundary
edges must be repaired before child graphs serve approximate search independently.

The reciprocal merge primitive accepts only adjacent spans with identical replicas. A live
merge still needs a coordinated Raft barrier before it can publish that descriptor change.
