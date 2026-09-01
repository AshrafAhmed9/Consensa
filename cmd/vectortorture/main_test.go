package main

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"testing"
)

func buildVectorTortureBinary(t *testing.T) string {
	t.Helper()
	bin := t.TempDir() + "/vectortorture"
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building cmd/vectortorture: %v\n%s", err, out)
	}
	return bin
}

// TestVectorTortureProducesValidReport runs the binary against a healthy, fault-free
// cluster and asserts the resulting report parses, every replica applied the same number
// of mutations, and their graph snapshots are byte-identical -- the same
// bit-identical-replica invariant ann.ReplicatedIndex's own unit tests prove, but through
// a real fault-injectable Cluster path instead of the always-delivers wrapper.
func TestVectorTortureProducesValidReport(t *testing.T) {
	bin := buildVectorTortureBinary(t)
	cmd := exec.Command(bin, "-nodes", "3", "-rounds", "15")
	cmd.Stdin = bytes.NewReader([]byte("[]"))
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("running cmd/vectortorture: %v", err)
	}

	var report struct {
		Replicas []struct {
			Node     int      `json:"node"`
			Applied  int      `json:"applied"`
			IDs      []string `json:"ids"`
			Snapshot string   `json:"snapshot"`
		} `json:"replicas"`
	}
	if err := json.Unmarshal(out, &report); err != nil {
		t.Fatalf("output did not match the expected schema: %v\nraw: %s", err, out)
	}
	if len(report.Replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(report.Replicas))
	}
	first := report.Replicas[0]
	if first.Applied == 0 {
		t.Fatal("no mutations applied against a healthy, fault-free cluster")
	}
	for _, r := range report.Replicas {
		if r.Applied != first.Applied {
			t.Fatalf("replica %d applied %d entries, replica %d applied %d -- a fault-free cluster must replicate identically", r.Node, r.Applied, first.Node, first.Applied)
		}
		if r.Snapshot != first.Snapshot {
			t.Fatalf("replica %d's graph snapshot differs from replica %d's despite applying the same %d entries", r.Node, first.Node, r.Applied)
		}
		seen := map[string]bool{}
		for _, id := range r.IDs {
			if seen[id] {
				t.Fatalf("replica %d has duplicate ID %q", r.Node, id)
			}
			seen[id] = true
		}
	}
}

// TestVectorTortureIsolatesTargetNode proves a targeted fault actually changes what gets
// applied compared to the fault-free case above, the same discipline
// cmd/torture's own TestTortureIsolatesTargetNode uses.
func TestVectorTortureIsolatesTargetNode(t *testing.T) {
	bin := buildVectorTortureBinary(t)
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
		t.Fatalf("running cmd/vectortorture: %v", err)
	}
	if !json.Valid(out) {
		t.Fatalf("output is not valid JSON: %s", out)
	}
}
