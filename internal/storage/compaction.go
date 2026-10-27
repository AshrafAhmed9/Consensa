package storage

import (
	"os"
	"sort"
)

// compact merges all SSTables into one once there are too many, so reads don't have to
// check a long list of files. When a key is in several tables, the newest value is kept.
func (d *DB) compact() error {
	if len(d.tables) < d.compactionTrigger {
		return nil
	}
	latest := make(map[string]record)
	for _, t := range d.tables { // d.tables is newest first
		for _, r := range t.records {
			if _, ok := latest[string(r.key)]; !ok {
				latest[string(r.key)] = r
			}
		}
	}
	merged := make([]record, 0, len(latest))
	for _, r := range latest {
		merged = append(merged, r)
	}
	sort.Slice(merged, func(i, j int) bool { return string(merged[i].key) < string(merged[j].key) })
	d.generation++
	t, err := writeSSTable(d.dir, d.generation, merged)
	if err != nil {
		return err
	}
	for _, old := range d.tables {
		if err := os.Remove(old.path); err != nil {
			return err
		}
	}
	d.tables = []*sstable{t}
	return nil
}
