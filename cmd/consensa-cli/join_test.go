package main

import "testing"

// TestParseIDAddrList proves the "id=host:port,..." parser (shared shape with
// cmd/consensa's own --peers convention) accepts well-formed input and rejects the
// malformed shapes an operator is most likely to actually type: a missing "=", an empty
// side, a non-numeric or zero ID, and an empty string entirely.
func TestParseIDAddrList(t *testing.T) {
	got, err := parseIDAddrList("1=127.0.0.1:8081,2=127.0.0.1:8082,3=127.0.0.1:8083")
	if err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}
	want := map[uint64]string{1: "127.0.0.1:8081", 2: "127.0.0.1:8082", 3: "127.0.0.1:8083"}
	if len(got) != len(want) {
		t.Fatalf("parsed %d entries, want %d", len(got), len(want))
	}
	for id, addr := range want {
		if got[id] != addr {
			t.Fatalf("entry %d = %q, want %q", id, got[id], addr)
		}
	}

	invalid := []string{
		"",
		"1",
		"1=",
		"=127.0.0.1:8081",
		"abc=127.0.0.1:8081",
		"0=127.0.0.1:8081",
	}
	for _, in := range invalid {
		if _, err := parseIDAddrList(in); err == nil {
			t.Fatalf("parseIDAddrList(%q) accepted malformed input", in)
		}
	}
}
