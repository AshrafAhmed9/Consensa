# ADR 006: implement snapshot-isolation transaction records

Batch vector upserts can cross static ranges, so an atomic boundary has practical value.
Consensa therefore uses HLC timestamps, write intents, and a small two-phase coordinator.
This phase intentionally provides snapshot isolation, not serializability: preventing
write skew requires read tracking and timestamp-cache work reserved for Phase 14.
