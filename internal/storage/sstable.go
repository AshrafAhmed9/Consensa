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
	f, e := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if e != nil {
		return nil, e
	}
	defer func() { _ = f.Close() }()
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
