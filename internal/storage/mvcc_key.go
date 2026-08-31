package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// HLC is an orderable hybrid-logical timestamp. Phase 1 treats it as caller-supplied;
// generation and clock-skew handling belong to the transaction layer.
type HLC struct {
	WallTime int64
	Logical  int32
}

func (h HLC) compare(other HLC) int {
	if h.WallTime < other.WallTime {
		return -1
	}
	if h.WallTime > other.WallTime {
		return 1
	}
	if h.Logical < other.Logical {
		return -1
	}
	if h.Logical > other.Logical {
		return 1
	}
	return 0
}

// MVCCKey combines a user key and timestamp for the internal sorted order.
type MVCCKey struct {
	Key       []byte
	Timestamp HLC
}

func encodeMVCCKey(k MVCCKey) []byte {
	b := make([]byte, len(k.Key)+1+12)
	copy(b, k.Key)
	b[len(k.Key)] = 0
	binary.BigEndian.PutUint64(b[len(k.Key)+1:len(k.Key)+9], ^uint64(k.Timestamp.WallTime))
	binary.BigEndian.PutUint32(b[len(k.Key)+9:len(k.Key)+13], ^uint32(k.Timestamp.Logical))
	return b
}
func decodeMVCCKey(b []byte) (MVCCKey, error) {
	if len(b) < 13 {
		return MVCCKey{}, errors.New("storage: invalid MVCC key")
	}
	i := len(b) - 13
	if b[i] != 0 {
		return MVCCKey{}, errors.New("storage: missing MVCC separator")
	}
	return MVCCKey{Key: append([]byte(nil), b[:i]...), Timestamp: HLC{WallTime: int64(^binary.BigEndian.Uint64(b[i+1 : i+9])), Logical: int32(^binary.BigEndian.Uint32(b[i+9:]))}}, nil
}
func sameUserKey(a, b []byte) bool { return bytes.Equal(a, b) }
