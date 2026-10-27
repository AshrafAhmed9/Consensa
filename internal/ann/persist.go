package ann

import (
	"encoding/json"
	"errors"

	"github.com/ashraf/consensa/internal/vector"
)

// Mutation is the deterministic payload a Raft entry carries for one graph operation.
// Replicas apply it in log order, so their graph topology does not depend on local timing.
type Mutation struct {
	Operation string        `json:"operation"`
	ID        string        `json:"id"`
	Vector    vector.Vector `json:"vector,omitempty"`
}

// EncodeMutation serializes a validated insertion for a Raft log entry.
func EncodeMutation(id string, v vector.Vector) ([]byte, error) {
	return json.Marshal(Mutation{Operation: "insert", ID: id, Vector: v})
}

// EncodeDeleteMutation serializes a replicated graph deletion.
func EncodeDeleteMutation(id string) ([]byte, error) {
	return json.Marshal(Mutation{Operation: "delete", ID: id})
}

// ApplyMutation decodes and applies one Raft-ordered graph mutation.
func (h *HNSW) ApplyMutation(data []byte) error {
	var m Mutation
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	switch m.Operation {
	case "insert":
		return h.Insert(m.ID, m.Vector)
	case "delete":
		return h.Delete(m.ID)
	default:
		return errors.New("ann: unknown graph mutation")
	}
}
