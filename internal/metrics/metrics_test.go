package metrics

import "testing"

// TestRegistryGathersSignals proves the default operational surface can be scraped.
func TestRegistryGathersSignals(t *testing.T) {
	m := NewRegistry()
	m.RaftTerm.Set(2)
	families, e := m.Registry.Gather()
	if e != nil || len(families) != 3 {
		t.Fatalf("gather=%d,%v", len(families), e)
	}
}
