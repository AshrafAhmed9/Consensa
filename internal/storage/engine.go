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
	"time"
)

// ErrNotFound distinguishes an absent historical version from a present tombstone.
var ErrNotFound = errors.New("storage: key not found")

// Options controls durability and the memory threshold. SyncEvery=1 fsyncs every write;
// larger values trade acknowledged-write durability for throughput and are explicit.
type Options struct {
	Dir                string
	SyncEvery          int
	MemtableMaxEntries int
	CompactionTrigger  int
}

// Engine is the storage contract consumed by upper layers. Its timestamp argument makes
// historical visibility explicit now, rather than requiring an invasive API change when
// transactions arrive in Phase 8.
type Engine interface {
	Put(key []byte, ts HLC, value []byte) error
	Get(key []byte, ts HLC) ([]byte, error)
	Delete(key []byte, ts HLC) error
	Scan(start, end []byte, ts HLC) Iterator
	Flush() error
	Close() error
}

// DB is a single-node MVCC LSM store. Its mutex makes the public API safe for callers,
// though consensus—not this mutex—will eventually define distributed write ordering.
type DB struct {
	mu                            sync.RWMutex
	dir                           string
	wal                           *wal
	mem                           *memtable
	tables                        []*sstable
	generation                    uint64
	maxEntries, compactionTrigger int
	closed                        bool
}

var _ Engine = (*DB)(nil)

// Open recovers an existing database before it accepts any request.
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
	d := &DB{dir: o.Dir, mem: newMemtable(), maxEntries: o.MemtableMaxEntries, compactionTrigger: o.CompactionTrigger}
	paths, err := filepath.Glob(filepath.Join(o.Dir, "sst-*.sst"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	for _, p := range paths {
		t, e := openSSTable(p)
		if e != nil {
			return nil, e
		}
		d.tables = append([]*sstable{t}, d.tables...)
		n := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(p), "sst-"), ".sst")
		if g, e := strconv.ParseUint(n, 10, 64); e == nil && g > d.generation {
			d.generation = g
		}
	}
	d.wal, err = openWAL(o.Dir, o.SyncEvery)
	if err != nil {
		return nil, err
	}
	if err = d.wal.replay(func(r walRecord) { d.mem.put(r.key, r.value, r.tombstone) }); err != nil {
		_ = d.wal.close()
		return nil, err
	}
	return d, nil
}

// Put writes a version. The WAL precedes memory so recovery cannot expose an acknowledged
// mutation that existed only in RAM.
func (d *DB) Put(key []byte, ts HLC, value []byte) error { return d.write(key, ts, value, false) }

// Delete writes a tombstone version; absence and deletion must remain distinct for MVCC.
func (d *DB) Delete(key []byte, ts HLC) error { return d.write(key, ts, nil, true) }
func (d *DB) write(key []byte, ts HLC, value []byte, tombstone bool) error {
	started := time.Now()
	defer func() { metrics.latency.WithLabelValues("write").Observe(time.Since(started).Seconds()) }()
	if len(key) == 0 {
		return errors.New("storage: empty key")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return errors.New("storage: closed")
	}
	k := encodeMVCCKey(MVCCKey{Key: key, Timestamp: ts})
	if err := d.wal.append(walRecord{key: k, value: value, tombstone: tombstone}); err != nil {
		return err
	}
	d.mem.put(k, value, tombstone)
	if d.mem.list.size >= d.maxEntries {
		err := d.flushLocked()
		if err == nil {
			metrics.operations.WithLabelValues("write").Inc()
		}
		return err
	}
	metrics.operations.WithLabelValues("write").Inc()
	return nil
}

// Get returns the newest version not later than ts.
func (d *DB) Get(key []byte, ts HLC) ([]byte, error) {
	started := time.Now()
	defer func() {
		metrics.operations.WithLabelValues("read").Inc()
		metrics.latency.WithLabelValues("read").Observe(time.Since(started).Seconds())
	}()
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, errors.New("storage: closed")
	}
	for _, r := range d.allRecordsNewest() {
		mk, e := decodeMVCCKey(r.key)
		if e != nil {
			return nil, e
		}
		if sameUserKey(mk.Key, key) && mk.Timestamp.compare(ts) <= 0 {
			if r.tombstone {
				return nil, ErrNotFound
			}
			return append([]byte(nil), r.value...), nil
		}
	}
	return nil, ErrNotFound
}

// Iterator visits a stable scan snapshot in key order.
type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
	Close() error
}

// Scan returns each newest visible user-key version in [start,end).
func (d *DB) Scan(start, end []byte, ts HLC) Iterator {
	d.mu.RLock()
	defer d.mu.RUnlock()
	seen := map[string]bool{}
	var out []record
	for _, r := range d.allRecordsNewest() {
		mk, e := decodeMVCCKey(r.key)
		if e != nil {
			continue
		}
		if bytes.Compare(mk.Key, start) < 0 || (len(end) > 0 && bytes.Compare(mk.Key, end) >= 0) || mk.Timestamp.compare(ts) > 0 || seen[string(mk.Key)] {
			continue
		}
		seen[string(mk.Key)] = true
		if !r.tombstone {
			out = append(out, record{key: mk.Key, value: r.value})
		}
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].key, out[j].key) < 0 })
	return &sliceIterator{records: out, index: -1}
}
func (d *DB) allRecordsNewest() []record {
	out := d.mem.list.all()
	for _, t := range d.tables {
		out = append(out, t.records...)
	}
	return out
}

// Flush writes the current memtable into a durable immutable table and starts a fresh WAL.
func (d *DB) Flush() error { d.mu.Lock(); defer d.mu.Unlock(); return d.flushLocked() }
func (d *DB) flushLocked() error {
	started := time.Now()
	defer func() { metrics.latency.WithLabelValues("flush").Observe(time.Since(started).Seconds()) }()
	rs := d.mem.list.all()
	if len(rs) == 0 {
		return d.wal.sync()
	}
	if err := d.wal.sync(); err != nil {
		return err
	}
	d.generation++
	t, err := writeSSTable(d.dir, d.generation, rs)
	if err != nil {
		return err
	}
	d.tables = append([]*sstable{t}, d.tables...)
	d.mem = newMemtable()
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
	metrics.operations.WithLabelValues("flush").Inc()
	return d.compact()
}

// Close flushes buffered writes; callers must not use DB after Close.
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

type sliceIterator struct {
	records []record
	index   int
	err     error
}

func (i *sliceIterator) Next() bool { i.index++; return i.index < len(i.records) }
func (i *sliceIterator) Key() []byte {
	if i.index < 0 || i.index >= len(i.records) {
		return nil
	}
	return append([]byte(nil), i.records[i.index].key...)
}
func (i *sliceIterator) Value() []byte {
	if i.index < 0 || i.index >= len(i.records) {
		return nil
	}
	return append([]byte(nil), i.records[i.index].value...)
}
func (i *sliceIterator) Err() error   { return i.err }
func (i *sliceIterator) Close() error { return nil }
