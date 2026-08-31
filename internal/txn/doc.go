// Package txn implements the timestamp and intent rules needed for atomic writes across
// ranges. It provides snapshot isolation first: readers see a stable timestamp and writes
// are installed provisionally until a transaction record decides their fate. Serializable
// conflict prevention is deliberately added later in Phase 14.
package txn
