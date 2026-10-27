package storage

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkSequentialWrite(b *testing.B) {
	db, err := Open(Options{Dir: b.TempDir(), SyncEvery: 1, MemtableMaxEntries: 4096})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key-%020d", i)), []byte("value")); err != nil {
			b.Fatal(err)
		}
	}
}

// TestRandomOperationsAgainstModel writes random values and checks every key against a
// plain Go map after each write, closing and reopening the DB now and then. The tiny
// memtable forces many flushes and compactions along the way.
func TestRandomOperationsAgainstModel(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, SyncEvery: 1, MemtableMaxEntries: 7})
	if err != nil {
		t.Fatal(err)
	}
	model := map[string]string{}
	rng := rand.New(rand.NewPCG(3, 4))
	for n := 1; n <= 1000; n++ {
		k := string(rune('a' + rng.IntN(6)))
		v := string(rune('A' + rng.IntN(26)))
		model[k] = v
		if err := d.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		for key := range model {
			got, err := d.Get([]byte(key))
			if err != nil || string(got) != model[key] {
				t.Fatalf("%q after write %d: got %q, %v want %q", key, n, got, err, model[key])
			}
		}
		if n%101 == 0 {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			if d, err = Open(Options{Dir: dir, SyncEvery: 1, MemtableMaxEntries: 7}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestGetMissingKey checks that an absent key reports ErrNotFound.
func TestGetMissingKey(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Get([]byte("nope")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing key = %v, want ErrNotFound", err)
	}
}

// TestPartialTrailingWALRecord proves a crash in the middle of a write loses only that
// write. A half-written record at the end of the WAL is normal after a crash, so it must
// not be treated as corruption.
func TestPartialTrailingWALRecord(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Put([]byte("a"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err = d.wal.close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	d, err = Open(Options{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err := d.Get([]byte("a"))
	if err != nil || string(got) != "one" {
		t.Fatalf("recovery = %q, %v", got, err)
	}
	_ = d.Close()
}

// TestScanReturnsNewestValuesInRange checks that Scan keeps only the newest value of
// each key, only keys inside [start, end), and in sorted order.
func TestScanReturnsNewestValuesInRange(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), SyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	_ = d.Put([]byte("a"), []byte("old"))
	_ = d.Put([]byte("a"), []byte("new"))
	_ = d.Put([]byte("b"), []byte("b"))
	_ = d.Put([]byte("z"), []byte("outside"))
	got := d.Scan([]byte("a"), []byte("z"))
	if len(got) != 2 || string(got[0].Key) != "a" || string(got[0].Value) != "new" || string(got[1].Key) != "b" {
		t.Fatalf("Scan = %v", got)
	}
}
