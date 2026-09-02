package kv

import (
	"bytes"
	"fmt"
	"time"
)

// splitTarget is the subset of *DurableRange ExecuteLiveSplit needs from each child --
// declared narrowly (matching internal/txn.rangeClient's own reasoning) so this file
// documents exactly what it touches independent of whatever else DurableRange grows.
type splitTarget interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
}

// ExecuteLiveSplit performs a real live split of an already-running parent range into two
// fresh child ranges, using the "rebuild from scratch" strategy this project's own docs
// (docs/notes/12-split-repair.md) name as the simplest of three documented options -- not
// the cheapest: incremental repair or serve-stale-parent would avoid the latency cliff
// every key pays here, and remain future work, stated there rather than hidden.
//
// It performs the SplitDescriptor math and migrates every key in parentData by proposing
// it into whichever child descriptor now owns it, confirming each write is actually
// visible before moving to the next key -- the same reason
// internal/txn.DurableStore.putAndConfirm exists: DurableRange.Put only proposes, it does
// not wait for commit, and a caller that needs "this call's effect is visible before the
// next one starts" (migrating in a defined order, so a resumed/retried split can tell
// what's already landed) cannot rely on a bare Put. It does NOT call Meta.Replace itself
// -- the caller decides exactly when the new topology becomes visible to routing, which
// matters because that must only happen once migration is confirmed complete, never
// before (see cmd/consensa's own usage).
//
// left/right must already be live, already-running ranges sharing the parent's replica
// set (or as much as one process's own local replica of one can be -- see cmd/consensa's
// newKVRange for the production shape that constructs one on the same shared transport,
// needing no new listener or address). ExecuteLiveSplit only knows how to migrate data
// into them, not how to construct them, since that wiring is deployment-specific.
//
// Put is retried against a bounded per-key timeout rather than failing on the first "not
// leader" error: only whichever process's local child replica currently leads the new
// group can accept a write, which may not be this process and may not have settled its
// first election yet -- the same "propose against every range, only the real leader's
// call has any effect" pattern advanceClosedTimestamps and maintainLeases (cmd/consensa)
// already use, extended here to also tolerate an election still in progress.
func ExecuteLiveSplit(parentDescriptor Descriptor, parentData map[string][]byte, splitKey []byte, leftID, rightID uint64, left, right splitTarget, perKeyTimeout time.Duration) (leftDescriptor, rightDescriptor Descriptor, err error) {
	leftDescriptor, rightDescriptor, err = SplitDescriptor(parentDescriptor, splitKey, leftID, rightID)
	if err != nil {
		return Descriptor{}, Descriptor{}, err
	}
	for k, v := range parentData {
		var target splitTarget
		switch {
		case leftDescriptor.Contains([]byte(k)):
			target = left
		case rightDescriptor.Contains([]byte(k)):
			target = right
		default:
			return Descriptor{}, Descriptor{}, fmt.Errorf("kv: key %q belongs to neither child descriptor -- split boundary gap", k)
		}
		if err := putAndConfirm(target, []byte(k), v, perKeyTimeout); err != nil {
			return Descriptor{}, Descriptor{}, fmt.Errorf("migrating key %q: %w", k, err)
		}
	}
	return leftDescriptor, rightDescriptor, nil
}

// putAndConfirm proposes key/value against target, retrying past a "not leader" error
// (this process's local replica may not yet -- or ever -- lead the new group) until the
// write is actually confirmed visible via Get, or perKeyTimeout elapses.
//
// Get is checked on every iteration, not only after a locally successful Put: when
// multiple processes independently call ExecuteLiveSplit against their own local replica
// of the same child group (cmd/consensa's design -- see its own doc comment), only
// whichever one is actually leader can ever make Put succeed, but the value it commits
// still replicates to every other process's local replica, including ones whose own Put
// calls keep failing "not leader" for the group's entire lifetime. Checking Get only
// inside the Put-succeeded branch would make a non-leader process spin until
// perKeyTimeout on every single key even though the real leader (a different process)
// already finished the migration -- found as a real bug via a CI failure where the
// actual leader logged "live split executed" within a second while the other two
// processes kept retrying "not leader" for the rest of the test's deadline, since they
// never once checked whether the key had already arrived by replication.
func putAndConfirm(target splitTarget, key, value []byte, perKeyTimeout time.Duration) error {
	deadline := time.Now().Add(perKeyTimeout)
	var lastErr error
	for {
		if err := target.Put(key, value); err != nil {
			lastErr = err
		}
		if got, err := target.Get(key); err == nil && bytes.Equal(got, value) {
			return nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return lastErr
			}
			return fmt.Errorf("kv: write to %q did not become visible before the deadline", key)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
