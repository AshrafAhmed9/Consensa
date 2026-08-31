# ADR 004: Use size-tiered compaction for the first storage engine

## Decision

Phase 1 merges similarly sized immutable SSTables after four tables accumulate. It does
not implement leveled compaction.

## Rationale

Size-tiered compaction is short enough to audit and keeps the first engine's write path
legible. Later range splitting bounds the data owned by any one node, so leveled
compaction's write-amplification tradeoff is not yet justified. This is not a claim that
size-tiered is universally superior; benchmarks must show a reason before revisiting it.
