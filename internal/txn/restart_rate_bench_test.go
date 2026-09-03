package txn

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// restartRateWorkload runs a fixed, seeded sequence of contended read-write transactions
// against a fresh Store and reports the fraction that could not commit -- either because
// WriteIntent's observed-read check rejected the write outright (no refresh) or because a
// refresh/uncertainty-restart attempt was itself exhausted. A small keyspace relative to
// transaction count is what manufactures real contention: with only keyspace keys, distinct
// transactions frequently pick overlapping read/write pairs, the same read-write edge
// TestWriteIntentRejectsWriteSkew constructs by hand for a single pair, here at volume and
// with randomized timestamps standing in for concurrent arrival order.
//
// refresh selects whether a conflict is retried through Coordinator.Prepare's read-refresh
// path (matching production's actual behavior) or treated as an immediate abort by calling
// Store.WriteIntent directly (the naive pre-refresh behavior this codebase had before
// docs/notes/14-serializable.md's read-refresh update). uncertainty selects whether reads go
// through ReadAtTimestamp with a nonzero maxOffset (restarting once past a value inside the
// clock-uncertainty window, per this session's uncertainty-interval work) or through plain
// Get (pre-uncertainty-interval behavior).
func restartRateWorkload(t testing.TB, seed int64, keyspace, txns int, refresh, uncertainty bool) (restarted, total int) {
	t.Helper()
	const maxOffset = 20 * time.Millisecond
	store := NewStore()
	if uncertainty {
		store.SetMaxOffset(maxOffset)
	}
	keys := make([]string, keyspace)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
		store.values[keys[i]] = []byte("v0")
	}

	rng := rand.New(rand.NewSource(seed))
	base := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return base }

	for i := 0; i < txns; i++ {
		readKey := keys[rng.Intn(keyspace)]
		writeKey := keys[rng.Intn(keyspace)]
		// Randomized, occasionally out-of-order timestamps within a bounded jitter window
		// stand in for concurrent transactions racing to arrive -- a strictly increasing
		// clock per transaction would never manufacture the read-after-write /
		// write-after-read collisions this benchmark exists to measure.
		jitter := time.Duration(rng.Int63n(int64(50 * time.Millisecond)))
		ts := Timestamp{WallTime: clock().Add(jitter).UnixNano(), Logical: int32(i)}

		total++

		var readErr error
		if uncertainty {
			_, readErr = store.ReadAtTimestamp([]byte(readKey), ts)
			if readErr != nil {
				restartTS := store.UncertaintyRestartTimestamp([]byte(readKey))
				_, readErr = store.ReadAtTimestamp([]byte(readKey), restartTS)
				if readErr == nil {
					ts = restartTS
				}
			}
		} else {
			_, readErr = store.Get([]byte(readKey))
		}
		if readErr != nil {
			restarted++
			continue
		}
		store.RecordRead([]byte(readKey), ts)

		txnID := fmt.Sprintf("t%d", i)
		intent := Intent{Key: []byte(writeKey), Value: []byte(fmt.Sprintf("v%d", i)), TxnID: txnID, Timestamp: ts}

		if !refresh {
			if err := store.WriteIntent(intent); err != nil {
				restarted++
				continue
			}
			_ = store.Resolve(Record{ID: txnID, Status: Committed, WriteTimestamp: ts})
			continue
		}

		coord := NewCoordinator(NewClock(clock))
		prepared, err := coord.Prepare(txnID, []WriteSet{{
			Store:   store,
			Intents: []Intent{intent},
			Reads:   [][]byte{[]byte(readKey)},
		}})
		if err != nil {
			restarted++
			continue
		}
		if err := coord.CommitRecord(prepared); err != nil {
			restarted++
			continue
		}
		_ = coord.Resolve(prepared)
	}
	return restarted, total
}

// TestTransactionRestartRateBenchmark measures and prints the real restart/abort rate under
// a fixed contended workload across all four combinations of {read-refresh on/off} x
// {uncertainty-intervals on/off} -- the comparison docs/notes/14-serializable.md's Phase 14
// DoD calls for: read-refresh's abort cost measured, and the before/after cost of adding
// uncertainty intervals measured. This is a correctness-adjacent measurement, not a
// microbenchmark of throughput, so it runs as a normal test (deterministic seed, asserted
// nothing except printing the real numbers) rather than go test -bench, matching how this
// package already treats "prove a property against real numbers" work (see
// TestWriteIntentRejectsWriteSkew et al.).
func TestTransactionRestartRateBenchmark(t *testing.T) {
	const keyspace = 8
	const txns = 2000
	const seed = 42

	type config struct {
		name                 string
		refresh, uncertainty bool
	}
	configs := []config{
		{"naive-abort, no uncertainty (pre-Phase-14 baseline)", false, false},
		{"read-refresh, no uncertainty", true, false},
		{"naive-abort, with uncertainty intervals", false, true},
		{"read-refresh, with uncertainty intervals (current production behavior)", true, true},
	}

	t.Logf("%-70s %10s %10s %8s", "configuration", "restarted", "total", "rate")
	results := make(map[string]float64, len(configs))
	for _, c := range configs {
		restarted, total := restartRateWorkload(t, seed, keyspace, txns, c.refresh, c.uncertainty)
		rate := float64(restarted) / float64(total)
		results[c.name] = rate
		t.Logf("%-70s %10d %10d %7.1f%%", c.name, restarted, total, rate*100)
	}

	naive := results["naive-abort, no uncertainty (pre-Phase-14 baseline)"]
	refreshed := results["read-refresh, no uncertainty"]
	if refreshed > naive {
		t.Errorf("read-refresh made the restart rate WORSE than naive abort: %.1f%% -> %.1f%% -- refresh should only ever reduce or match abort rate, never increase it", naive*100, refreshed*100)
	}
	t.Logf("read-refresh effect (no uncertainty): %.1f%% -> %.1f%% restart rate (%.1f pp %s)",
		naive*100, refreshed*100, abs(naive-refreshed)*100, direction(naive, refreshed))

	beforeUncertainty := results["read-refresh, no uncertainty"]
	afterUncertainty := results["read-refresh, with uncertainty intervals (current production behavior)"]
	t.Logf("uncertainty-interval effect (with read-refresh): %.1f%% -> %.1f%% restart rate (%.1f pp %s)",
		beforeUncertainty*100, afterUncertainty*100, abs(beforeUncertainty-afterUncertainty)*100, direction(beforeUncertainty, afterUncertainty))
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

func direction(before, after float64) string {
	if after > before {
		return "increase"
	}
	if after < before {
		return "decrease"
	}
	return "no change"
}
