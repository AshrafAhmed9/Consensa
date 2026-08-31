package storage

import (
	"os"
	"sort"
	"time"
)

// compact merges all immutable tables. This is the smallest correct size-tiered policy:
// once enough similarly sized files exist, rewriting them bounds read amplification.
func (e *DB) compact() error {
	started := time.Now()
	defer func() { metrics.latency.WithLabelValues("compaction").Observe(time.Since(started).Seconds()) }()
	if len(e.tables) < e.compactionTrigger {
		return nil
	}
	latest := make(map[string]record)
	for _, t := range e.tables {
		for _, r := range t.records {
			if _, ok := latest[string(r.key)]; !ok {
				latest[string(r.key)] = r
			}
		}
	}
	rs := make([]record, 0, len(latest))
	for _, r := range latest {
		rs = append(rs, r)
	}
	sort.Slice(rs, func(i, j int) bool { return string(rs[i].key) < string(rs[j].key) })
	e.generation++
	t, err := writeSSTable(e.dir, e.generation, rs)
	if err != nil {
		return err
	}
	for _, old := range e.tables {
		if err := os.Remove(old.path); err != nil {
			return err
		}
	}
	e.tables = []*sstable{t}
	metrics.operations.WithLabelValues("compaction").Inc()
	return nil
}
