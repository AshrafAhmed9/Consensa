package raft

import (
	"bufio"
	"bytes"
	"testing"
	"time"
)

// TestTCPTransportDeliversOneFramedMessage proves the real socket adapter preserves a
// complete Raft message, including entry bytes, without exposing partial TCP reads.
func TestTCPTransportDeliversOneFramedMessage(t *testing.T) {
	received := make(chan Message, 1)
	receiver, err := ListenTCP(2, "127.0.0.1:0", nil, func(message Message) error {
		received <- message
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	sender, err := ListenTCP(1, "127.0.0.1:0", map[NodeID]string{2: receiver.Addr().String()}, func(Message) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	want := Message{Type: MsgAppend, From: 1, To: 2, Term: 4, Entries: []Entry{{Index: 3, Term: 4, Data: []byte("command")}}}
	if err := sender.Send(want); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if got.Type != want.Type || got.From != want.From || got.To != want.To || got.Term != want.Term || len(got.Entries) != 1 || !bytes.Equal(got.Entries[0].Data, want.Entries[0].Data) {
			t.Fatalf("message = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for framed message")
	}
}

// TestReadFrameRejectsOversizedPayload proves an attacker-controlled length prefix cannot
// force a node to allocate an arbitrary amount of memory.
func TestReadFrameRejectsOversizedPayload(t *testing.T) {
	var frame [4]byte
	frame[0], frame[3] = 1, 1 // 16 MiB + one byte in big-endian form.
	if _, err := readFrame(bufio.NewReader(bytes.NewReader(frame[:]))); err == nil {
		t.Fatal("oversized frame accepted")
	}
}
