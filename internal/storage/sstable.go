package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// sstable is a sorted, immutable table of records, saved as one file. It is read fully
// into memory when opened, which keeps lookups simple (a binary search).
//
// File format: for each record, 4-byte key length, 4-byte value length, key, value.
type sstable struct {
	path    string
	records []record // sorted by key
}

// writeSSTable saves sorted records as generation number "generation". Higher
// generations are newer. It returns nil if there are no records.
func writeSSTable(dir string, generation uint64, records []record) (*sstable, error) {
	if len(records) == 0 {
		return nil, nil
	}
	p := filepath.Join(dir, fmt.Sprintf("sst-%020d.sst", generation))
	// Write to a temporary file and rename it into place. A crash mid-write then leaves
	// no half-written table, because a rename is all-or-nothing.
	tmp := p + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	for _, r := range records {
		header := make([]byte, 8)
		binary.BigEndian.PutUint32(header[:4], uint32(len(r.key)))
		binary.BigEndian.PutUint32(header[4:], uint32(len(r.value)))
		for _, chunk := range [][]byte{header, r.key, r.value} {
			if _, err = f.Write(chunk); err != nil {
				_ = f.Close()
				return nil, err
			}
		}
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	if err = os.Rename(tmp, p); err != nil {
		return nil, err
	}
	return &sstable{path: p, records: records}, nil
}

// openSSTable loads a table file.
func openSSTable(path string) (*sstable, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var records []record
	for len(data) > 0 {
		if len(data) < 8 {
			return nil, errors.New("storage: truncated SSTable")
		}
		k := int(binary.BigEndian.Uint32(data[:4]))
		v := int(binary.BigEndian.Uint32(data[4:8]))
		data = data[8:]
		if k+v > len(data) {
			return nil, errors.New("storage: invalid SSTable record")
		}
		records = append(records, record{key: append([]byte(nil), data[:k]...), value: append([]byte(nil), data[k:k+v]...)})
		data = data[k+v:]
	}
	return &sstable{path: path, records: records}, nil
}

// get looks up a key with a binary search.
func (t *sstable) get(key []byte) ([]byte, bool) {
	i := sort.Search(len(t.records), func(i int) bool { return string(t.records[i].key) >= string(key) })
	if i < len(t.records) && string(t.records[i].key) == string(key) {
		return t.records[i].value, true
	}
	return nil, false
}
