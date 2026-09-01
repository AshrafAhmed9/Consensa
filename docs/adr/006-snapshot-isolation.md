# ADR 006: implement snapshot-isolation transaction records

Batch vector upserts can cross static ranges, so an atomic boundary has practical value.
Consensa therefore uses HLC timestamps, write intents, and a small two-phase coordinator.
This phase intentionally provides snapshot isolation, not serializability: preventing
write skew requires read tracking and timestamp-cache work reserved for Phase 14.

**Update:** Phase 14's read tracking and timestamp cache are now wired into the in-memory
`Store` participant (`internal/txn/intent.go`, `TestWriteIntentRejectsWriteSkew`) -- see
`docs/notes/14-serializable.md` for what this closes and what it deliberately does not
(full SSI's permissive schedule analysis; `DurableStore`, the Raft-backed participant).
