package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

// wal is the write-ahead log: every write is appended here before it is acknowledged, so
// it can be replayed after a crash.
//
// Each record is: 4-byte length, 4-byte checksum, then the payload (4-byte key length,
// key, value). A half-written record at the very end is normal after a crash and is
// dropped. A full record with a bad checksum means the file is corrupt.
type wal struct {
	path      string
	file      *os.File
	syncEvery int // fsync after this many appends (1 = after every write)
	pending   int // appends since the last fsync
}

func openWAL(dir string, syncEvery int) (*wal, error) {
	p := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	return &wal{path: p, file: f, syncEvery: syncEvery}, nil
}

// append writes one record, and fsyncs if enough records are waiting.
func (w *wal) append(r record) error {
	payload := make([]byte, 4+len(r.key)+len(r.value))
	binary.BigEndian.PutUint32(payload[:4], uint32(len(r.key)))
	copy(payload[4:], r.key)
	copy(payload[4+len(r.key):], r.value)
	head := make([]byte, 8)
	binary.BigEndian.PutUint32(head[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(head[4:], crc32.ChecksumIEEE(payload))
	if _, err := w.file.Write(head); err != nil {
		return err
	}
	if _, err := w.file.Write(payload); err != nil {
		return err
	}
	w.pending++
	if w.syncEvery > 0 && w.pending >= w.syncEvery {
		return w.sync()
	}
	return nil
}

// sync forces everything written so far onto disk. It does nothing if nothing is waiting.
func (w *wal) sync() error {
	if w.pending == 0 {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.pending = 0
	return nil
}

// replay calls fn for every record in the log, in order.
func (w *wal) replay(fn func(record)) error {
	data, err := os.ReadFile(w.path)
	if err != nil {
		return err
	}
	offset := 0
	for offset < len(data) {
		start := offset
		if len(data)-offset < 8 {
			return os.Truncate(w.path, int64(start)) // partial header: drop it
		}
		n := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		sum := binary.BigEndian.Uint32(data[offset+4 : offset+8])
		offset += 8
		if len(data)-offset < n {
			return os.Truncate(w.path, int64(start)) // partial payload: drop it
		}
		p := data[offset : offset+n]
		offset += n
		if sum != crc32.ChecksumIEEE(p) {
			return errors.New("storage: WAL checksum mismatch")
		}
		if len(p) < 4 {
			return errors.New("storage: WAL record too short")
		}
		k := int(binary.BigEndian.Uint32(p[:4]))
		if 4+k > len(p) {
			return errors.New("storage: WAL key length invalid")
		}
		fn(record{key: append([]byte(nil), p[4:4+k]...), value: append([]byte(nil), p[4+k:]...)})
	}
	return nil
}

func (w *wal) close() error { return w.file.Close() }
