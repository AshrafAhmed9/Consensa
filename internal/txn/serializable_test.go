package txn

import "testing"

// TestTimestampCachePushesConflictingWrite proves a read cannot be followed by an earlier write.
func TestTimestampCachePushesConflictingWrite(t *testing.T) {
	c := NewTimestampCache()
	read := Timestamp{WallTime: 10}
	c.RecordRead([]byte("doctor-b"), read)
	got := c.PushWrite([]byte("doctor-b"), Timestamp{WallTime: 9})
	if got.Compare(read) <= 0 {
		t.Fatalf("write not pushed: %v", got)
	}
}
