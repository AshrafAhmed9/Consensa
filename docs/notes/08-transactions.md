# Phase 8: transactions

## Why does this exist?

An ingest batch spanning ranges must not become partially visible after a failure.

## How does it work?

An HLC assigns a timestamp. Participants write intents, then a record on the first
participant is the sole commit point. A reader that meets an intent consults that anchor:
a committed record exposes the intent immediately, while cleanup can occur later.

## What alternatives existed?

Independent per-range upserts are cheaper but cannot offer atomic visibility. A full
serializable protocol is stronger but requires conflict tracking beyond this phase.

## What tradeoff was made?

This is snapshot isolation: it keeps intent resolution explicit and testable, while Phase
14 later adds read tracking and refresh to eliminate write skew.

## What can fail?

The in-memory coordinator is a model until its decisions are Raft persisted. A crash in a
real system requires any participant to resolve abandoned intents from the record; the
explicit prepare/commit/resolve boundary is present so that recovery path is testable.
