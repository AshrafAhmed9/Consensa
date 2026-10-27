package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ErrNotFound means the key has never been written.
var ErrNotFound = errors.New("storage: key not found")

// Options configures a DB.
type Options struct {
	Dir                string // directory for the files (created if missing)
	SyncEvery          int    // fsync the WAL every N writes (1 = every write); 0 = only when Sync is called
	MemtableMaxEntries int    // flush the in-memory map to a table after this many keys (default 1024)
	CompactionTrigger  int    // merge tables once there are this many (default 4)
}

// KV is a key and its value, as returned by Scan.
type KV struct {
	Key, Value []byte
}

// DB is the key-value store. A mutex makes it safe to share between goroutines.
type DB struct {
	mu                            sync.RWMutex
	dir                           string
	wal                           *wal
	mem                           *skiplist  // recent writes not yet in a table
	tables                        []*sstable // newest first
	generation                    uint64     // number of the newest table file
	maxEntries, compactionTrigger int
	closed                        bool
}

// Open loads the tables, replays the WAL, and returns a ready DB.
func Open(o Options) (*DB, error) {
	if o.Dir == "" {
		return nil, errors.New("storage: Dir is required")
	}
	if o.MemtableMaxEntries <= 0 {
		o.MemtableMaxEntries = 1024
	}
	if o.CompactionTrigger <= 0 {
		o.CompactionTrigger = 4
	}
	if err := os.MkdirAll(o.Dir, 0700); err != nil {
		return nil, err
	}
	d := &DB{dir: o.Dir, mem: newSkiplist(), maxEntries: o.MemtableMaxEntries, compactionTrigger: o.CompactionTrigger}
	paths, err := filepath.Glob(filepath.Join(o.Dir, "sst-*.sst"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths) // names sort oldest to newest
	for _, p := range paths {
		t, err := openSSTable(p)
		if err != nil {
			return nil, err
		}
		d.tables = append([]*sstable{t}, d.tables...)
		name := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(p), "sst-"), ".sst")
		if g, err := strconv.ParseUint(name, 10, 64); err == nil && g > d.generation {
			d.generation = g
		}
	}
	d.wal, err = openWAL(o.Dir, o.SyncEvery)
	if err != nil {
		return nil, err
	}
	if err = d.wal.replay(func(r record) { d.mem.put(r.key, r.value) }); err != nil {
		_ = d.wal.close()
		return nil, err
	}
	return d, nil
}

// Put writes a value. It goes to the WAL first, so a crash cannot lose an acknowledged write.
func (d *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("storage: empty key")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return errors.New("storage: closed")
	}
	if err := d.wal.append(record{key: key, value: value}); err != nil {
		return err
	}
	d.mem.put(key, value)
	if d.mem.size >= d.maxEntries {
		return d.flushLocked()
	}
	return nil
}

// Get returns the value for key, or ErrNotFound. It checks the newest data first.
func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, errors.New("storage: closed")
	}
	if v, ok := d.mem.get(key); ok {
		return append([]byte(nil), v...), nil
	}
	for _, t := range d.tables {
		if v, ok := t.get(key); ok {
			return append([]byte(nil), v...), nil
		}
	}
	return nil, ErrNotFound
}

// Scan returns every key in [start, end) with its value, in key order.
// An empty end means "to the end".
func (d *DB) Scan(start, end []byte) []KV {
	d.mu.RLock()
	defer d.mu.RUnlock()
	latest := map[string][]byte{}
	for i := len(d.tables) - 1; i >= 0; i-- { // oldest table first, so newer values overwrite
		for _, r := range d.tables[i].records {
			latest[string(r.key)] = r.value
		}
	}
	for _, r := range d.mem.all() {
		latest[string(r.key)] = r.value
	}
	var out []KV
	for k, v := range latest {
		key := []byte(k)
		if bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0) {
			out = append(out, KV{Key: key, Value: append([]byte(nil), v...)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Key, out[j].Key) < 0 })
	return out
}

// Sync forces all writes so far onto disk (an fsync of the WAL). Use it with SyncEvery 0
// to make a batch of writes durable with one fsync instead of one per write.
func (d *DB) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return errors.New("storage: closed")
	}
	return d.wal.sync()
}

// Flush writes the in-memory data out to a new table and starts a fresh WAL.
func (d *DB) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.flushLocked()
}

// flushLocked writes the in-memory map to a new table and starts a fresh WAL.
// The caller must hold d.mu.
func (d *DB) flushLocked() error {
	records := d.mem.all()
	if err := d.wal.sync(); err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	d.generation++
	t, err := writeSSTable(d.dir, d.generation, records)
	if err != nil {
		return err
	}
	d.tables = append([]*sstable{t}, d.tables...)
	d.mem = newSkiplist()
	// The table now holds everything the WAL did, so the WAL can be thrown away.
	if err = d.wal.close(); err != nil {
		return err
	}
	if err = os.Remove(d.wal.path); err != nil {
		return err
	}
	d.wal, err = openWAL(d.dir, d.wal.syncEvery)
	if err != nil {
		return err
	}
	return d.compact()
}

// Close flushes remaining writes. The DB must not be used afterwards.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	if err := d.flushLocked(); err != nil {
		return err
	}
	d.closed = true
	return d.wal.close()
}
