package ann

import (
	"encoding/json"
	"errors"

	"github.com/ashraf/consensa/internal/vector"
)

// Mutation is what a Raft log entry holds for the vector index: an insert or a delete.
// Replicas apply them in log order, so they all build the same graph.
type Mutation struct {
	Operation string        `json:"operation"` // "insert" or "delete"
	ID        string        `json:"id"`
	Vector    vector.Vector `json:"vector,omitempty"` // only for insert
}

// EncodeMutation makes the log entry bytes for inserting (or replacing) a vector.
func EncodeMutation(id string, v vector.Vector) ([]byte, error) {
	return json.Marshal(Mutation{Operation: "insert", ID: id, Vector: v})
}

// EncodeDeleteMutation makes the log entry bytes for deleting a vector.
func EncodeDeleteMutation(id string) ([]byte, error) {
	return json.Marshal(Mutation{Operation: "delete", ID: id})
}

// ApplyMutation decodes one log entry and applies it to the graph.
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
