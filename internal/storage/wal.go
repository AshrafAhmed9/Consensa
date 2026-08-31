package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

type walRecord struct {
	key, value []byte
	tombstone  bool
}

// wal acknowledges only after the configured sync policy. A trailing partial record is
// normal after a crash and is discarded; a complete record with a bad checksum is corruption.
type wal struct {
	path      string
	file      *os.File
	syncEvery int
	pending   int
}

func openWAL(dir string, syncEvery int) (*wal, error) {
	p := filepath.Join(dir, "wal.log")
	f, e := os.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if e != nil {
		return nil, e
	}
	return &wal{path: p, file: f, syncEvery: syncEvery}, nil
}
func (w *wal) append(r walRecord) error {
	payload := make([]byte, 5+len(r.key)+len(r.value))
	if r.tombstone {
		payload[0] = 1
	}
	binary.BigEndian.PutUint32(payload[1:5], uint32(len(r.key)))
	copy(payload[5:], r.key)
	copy(payload[5+len(r.key):], r.value)
	head := make([]byte, 8)
	binary.BigEndian.PutUint32(head[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(head[4:], crc32.ChecksumIEEE(payload))
	if _, e := w.file.Write(head); e != nil {
		return e
	}
	if _, e := w.file.Write(payload); e != nil {
		return e
	}
	w.pending++
	if w.syncEvery > 0 && w.pending >= w.syncEvery {
		return w.sync()
	}
	return nil
}
func (w *wal) sync() error {
	if e := w.file.Sync(); e != nil {
		return e
	}
	w.pending = 0
	return nil
}
func (w *wal) replay(fn func(walRecord)) error {
	data, e := os.ReadFile(w.path)
	if e != nil {
		return e
	}
	offset := 0
	for offset < len(data) {
		start := offset
		if len(data)-offset < 8 {
			return os.Truncate(w.path, int64(start))
		}
		n := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		sum := binary.BigEndian.Uint32(data[offset+4 : offset+8])
		offset += 8
		if n < 0 || len(data)-offset < n {
			return os.Truncate(w.path, int64(start))
		}
		p := data[offset : offset+n]
		offset += n
		if sum != crc32.ChecksumIEEE(p) {
			return errors.New("storage: WAL checksum mismatch")
		}
		if len(p) < 5 {
			return errors.New("storage: WAL record too short")
		}
		k := int(binary.BigEndian.Uint32(p[1:5]))
		if k < 0 || 5+k > len(p) {
			return errors.New("storage: WAL key length invalid")
		}
		fn(walRecord{key: append([]byte(nil), p[5:5+k]...), value: append([]byte(nil), p[5+k:]...), tombstone: p[0] == 1})
	}
	return nil
}
func (w *wal) close() error { return w.file.Close() }
