package main

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"testing"
)

// These tests run the torture binary directly, without Python. They catch a changed JSON
// format or a crash on some fault schedule.

func buildTortureBinary(t *testing.T) string {
	t.Helper()
	bin := t.TempDir() + "/torture"
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building cmd/torture: %v\n%s", err, out)
	}
	return bin
}

// TestTortureProducesValidHistory runs with no faults and checks the output has the
// shape the Python checker expects, with both reads and writes, and that a fault-free
// read always returns a value.
func TestTortureProducesValidHistory(t *testing.T) {
	bin := buildTortureBinary(t)
	cmd := exec.Command(bin, "-nodes", "3", "-rounds", "15")
	cmd.Stdin = bytes.NewReader([]byte("[]")) // no fault schedule at all
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("running cmd/torture: %v", err)
	}

	var report struct {
		History []struct {
			Invocation int     `json:"invocation"`
			Response   int     `json:"response"`
			Kind       string  `json:"kind"`
			Value      *string `json:"value"`
			Result     *string `json:"result"`
		} `json:"history"`
	}
	if err := json.Unmarshal(out, &report); err != nil {
		t.Fatalf("output did not match the expected schema: %v\nraw: %s", err, out)
	}
	if len(report.History) == 0 {
		t.Fatal("no operations recorded against a healthy, fault-free cluster")
	}

	writes, reads := 0, 0
	for _, op := range report.History {
		switch op.Kind {
		case "write":
			writes++
			if op.Value == nil {
				t.Fatalf("write operation has nil value: %+v", op)
			}
			if op.Response <= op.Invocation {
				t.Fatalf("write response (%d) does not follow invocation (%d)", op.Response, op.Invocation)
			}
		case "read":
			reads++
			if op.Result == nil {
				t.Fatalf("read against a fault-free cluster returned nil result: %+v", op)
			}
		default:
			t.Fatalf("unknown operation kind %q", op.Kind)
		}
	}
	if writes == 0 || reads == 0 {
		t.Fatalf("expected both writes and reads against a fault-free cluster, got writes=%d reads=%d", writes, reads)
	}
}

// TestTortureRunsWithFaults checks that the binary accepts a fault schedule in the same
// JSON shape nemesis.py produces, finishes, and prints valid JSON. It does not check
// what the faults did to the history; the Python linearizability check does that.
func TestTortureRunsWithFaults(t *testing.T) {
	bin := buildTortureBinary(t)
	schedule, err := json.Marshal([]map[string]any{
		{"step": 3, "kind": "partition", "target": 0},
		{"step": 4, "kind": "partition", "target": 0},
		{"step": 5, "kind": "partition", "target": 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-nodes", "3", "-rounds", "15")
	cmd.Stdin = bytes.NewReader(schedule)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("running cmd/torture: %v", err)
	}
	if !json.Valid(out) {
		t.Fatalf("output is not valid JSON: %s", out)
	}
}
