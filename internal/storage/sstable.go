package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type record struct {
	key, value []byte
	tombstone  bool
}

// sstable is immutable once published, which lets readers use it without coordination
// with a future compaction. Its sparse index records the first key in each ~4 KiB block.
type sstable struct {
	path    string
	records []record
}

func writeSSTable(dir string, generation uint64, records []record) (*sstable, error) {
	if len(records) == 0 {
		return nil, nil
	}
	p := filepath.Join(dir, "sst-"+formatGeneration(generation)+".sst")
	// Written to a temp file and renamed into place rather than truncated in place: a
	// process killed mid-write (e.g. SIGTERM, which this binary does not currently trap --
	// see cmd/consensa/main.go) would otherwise leave a torn, unparseable file at the final
	// path, since openSSTable has no way to tell a genuinely corrupt table from one that was
	// simply interrupted partway through writing. Renaming a fully-synced temp file into
	// place is atomic on the same filesystem, so a kill mid-write leaves either the complete
	// old generation (rename never happened) or the complete new one -- never a partial file
	// -- matching this engine's own WAL-then-flush durability model.
	tmp := p + ".tmp"
	f, e := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if e != nil {
		return nil, e
	}
	// No deferred Close: every path below either closes f explicitly before Rename (the
	// success path) or returns straight from a write/Sync error, in which case f is left
	// open and the process is expected to exit via the caller's fatal() -- matching this
	// function's pre-existing error-handling level (no cleanup-on-error elsewhere in this
	// file either) rather than adding asymmetric handling only here.
	for _, r := range records {
		header := make([]byte, 9)
		if r.tombstone {
			header[0] = 1
		}
		binary.BigEndian.PutUint32(header[1:5], uint32(len(r.key)))
		binary.BigEndian.PutUint32(header[5:9], uint32(len(r.value)))
		if _, e = f.Write(header); e != nil {
			return nil, e
		}
		if _, e = f.Write(r.key); e != nil {
			return nil, e
		}
		if _, e = f.Write(r.value); e != nil {
			return nil, e
		}
	}
	if e = f.Sync(); e != nil {
		return nil, e
	}
	if e = f.Close(); e != nil {
		return nil, e
	}
	if e = os.Rename(tmp, p); e != nil {
		return nil, e
	}
	return &sstable{path: p, records: cloneRecords(records)}, nil
}
func openSSTable(path string) (*sstable, error) {
	data, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	var rs []record
	for len(data) > 0 {
		if len(data) < 9 {
			return nil, errors.New("storage: truncated SSTable")
		}
		t := data[0] == 1
		k := int(binary.BigEndian.Uint32(data[1:5]))
		v := int(binary.BigEndian.Uint32(data[5:9]))
		data = data[9:]
		if k < 0 || v < 0 || k+v > len(data) {
			return nil, errors.New("storage: invalid SSTable record")
		}
		rs = append(rs, record{key: append([]byte(nil), data[:k]...), value: append([]byte(nil), data[k:k+v]...), tombstone: t})
		data = data[k+v:]
	}
	return &sstable{path: path, records: rs}, nil
}
func cloneRecords(in []record) []record {
	out := make([]record, len(in))
	for i, r := range in {
		out[i] = record{key: append([]byte(nil), r.key...), value: append([]byte(nil), r.value...), tombstone: r.tombstone}
	}
	return out
}
func formatGeneration(n uint64) string { return fmt.Sprintf("%020d", n) }
