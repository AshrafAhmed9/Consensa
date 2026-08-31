package storage

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

func ts(n int64) HLC { return HLC{WallTime: n} }

func BenchmarkSequentialWrite(b *testing.B) {
	db, err := Open(Options{Dir: b.TempDir(), SyncEvery: 1, MemtableMaxEntries: 4096})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key-%020d", i)), ts(int64(i)), []byte("value")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPointRead(b *testing.B) {
	db, err := Open(Options{Dir: b.TempDir(), SyncEvery: 1, MemtableMaxEntries: 4096})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	const keys = 64
	for i := 0; i < keys; i++ {
		_ = db.Put([]byte(fmt.Sprintf("key-%020d", i)), ts(int64(i)), []byte("value"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("key-%020d", i%keys)), ts(keys)); err != nil {
			b.Fatal(err)
		}
	}
}

// TestRandomOperationsAgainstModel checks visibility after every mutation and restart.
// It catches ordering mistakes that example-driven tests tend to leave behind.
func TestRandomOperationsAgainstModel(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, SyncEvery: 1, MemtableMaxEntries: 7})
	if err != nil {
		t.Fatal(err)
	}
	model := map[string]map[int64]*string{}
	rng := rand.New(rand.NewPCG(3, 4))
	for n := int64(1); n <= 1000; n++ {
		k := string(rune('a' + rng.IntN(6)))
		if model[k] == nil {
			model[k] = map[int64]*string{}
		}
		if rng.IntN(4) == 0 {
			if err := d.Delete([]byte(k), ts(n)); err != nil {
				t.Fatal(err)
			}
			model[k][n] = nil
		} else {
			v := string(rune('A' + rng.IntN(26)))
			model[k][n] = &v
			if err := d.Put([]byte(k), ts(n), []byte(v)); err != nil {
				t.Fatal(err)
			}
		}
		for key, versions := range model {
			var best int64 = -1
			var expected *string
			for at, v := range versions {
				if at <= n && at > best {
					best, expected = at, v
				}
			}
			got, e := d.Get([]byte(key), ts(n))
			if expected == nil {
				if !errors.Is(e, ErrNotFound) {
					t.Fatalf("%q at %d: got %q, %v", key, n, got, e)
				}
			} else if e != nil || string(got) != *expected {
				t.Fatalf("%q at %d: got %q, %v want %q", key, n, got, e, *expected)
			}
		}
		if n%101 == 0 {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d, err = Open(Options{Dir: dir, SyncEvery: 1, MemtableMaxEntries: 7})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestPartialTrailingWALRecord proves a crash mid-write loses only the torn operation;
// treating this common crash state as corruption would make recovery brittle.
func TestPartialTrailingWALRecord(t *testing.T) {
	dir := t.TempDir()
	d, e := Open(Options{Dir: dir, SyncEvery: 1})
	if e != nil {
		t.Fatal(e)
	}
	if e = d.Put([]byte("a"), ts(1), []byte("one")); e != nil {
		t.Fatal(e)
	}
	if e = d.wal.close(); e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.Write([]byte{1, 2, 3}); e != nil {
		t.Fatal(e)
	}
	_ = f.Close()
	d, e = Open(Options{Dir: dir, SyncEvery: 1})
	if e != nil {
		t.Fatal(e)
	}
	got, e := d.Get([]byte("a"), ts(1))
	if e != nil || string(got) != "one" {
		t.Fatalf("recovery = %q, %v", got, e)
	}
	_ = d.Close()
}
func TestScanHonorsMVCCAndTombstones(t *testing.T) {
	d, e := Open(Options{Dir: t.TempDir(), SyncEvery: 1})
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	_ = d.Put([]byte("a"), ts(1), []byte("old"))
	_ = d.Put([]byte("a"), ts(2), []byte("new"))
	_ = d.Put([]byte("b"), ts(1), []byte("b"))
	_ = d.Delete([]byte("b"), ts(2))
	it := d.Scan([]byte("a"), []byte("z"), ts(2))
	defer it.Close()
	if !it.Next() || string(it.Key()) != "a" || string(it.Value()) != "new" || it.Next() {
		t.Fatal("scan visibility incorrect")
	}
}
