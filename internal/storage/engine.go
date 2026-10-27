package storage

import (
	"bytes"
	"errors"
	"os"
	"sort"
	"sync"
)

// ErrNotFound means the key has never been written.
var ErrNotFound = errors.New("storage: key not found")

// Options configures a DB.
type Options struct {
	Dir       string // directory for the log file (created if missing)
	SyncEvery int    // fsync the WAL every N writes (1 = every write); 0 = only when Sync is called
}

// KV is a key and its value, as returned by Scan.
type KV struct {
	Key, Value []byte
}

// DB is the key-value store. A mutex makes it safe to share between goroutines.
type DB struct {
	mu     sync.RWMutex
	wal    *wal
	data   map[string][]byte // the current value of every key
	closed bool
}

// Open replays the WAL (if there is one) and returns a ready DB.
func Open(o Options) (*DB, error) {
	if o.Dir == "" {
		return nil, errors.New("storage: Dir is required")
	}
	if err := os.MkdirAll(o.Dir, 0700); err != nil {
		return nil, err
	}
	w, err := openWAL(o.Dir, o.SyncEvery)
	if err != nil {
		return nil, err
	}
	d := &DB{wal: w, data: map[string][]byte{}}
	if err := w.replay(func(r record) { d.data[string(r.key)] = r.value }); err != nil {
		_ = w.close()
		return nil, err
	}
	return d, nil
}

// Put writes a value. It goes to the WAL first, so a crash cannot lose an acknowledged
// write (once synced).
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
	d.data[string(key)] = append([]byte(nil), value...)
	return nil
}

// Get returns the value for key, or ErrNotFound.
func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, errors.New("storage: closed")
	}
	v, ok := d.data[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

// Scan returns every key in [start, end) with its value, in key order.
func (d *DB) Scan(start, end []byte) []KV {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var out []KV
	for k, v := range d.data {
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

// Close syncs and closes the log. The DB must not be used afterwards.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	if err := d.wal.sync(); err != nil {
		_ = d.wal.close()
		return err
	}
	return d.wal.close()
}
