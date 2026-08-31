package ann

import "errors"

// Split clones the parent graph into two child graphs and repairs each child against its
// ownership predicate. Callers must ensure the predicates are disjoint and exhaustive for
// the parent keyspace before publishing the corresponding range descriptors.
func (h *HNSW) Split(leftOwns func(string) bool) (*HNSW, *HNSW, error) {
	if leftOwns == nil {
		return nil, nil, errors.New("ann: split ownership predicate is required")
	}
	snapshot, err := h.Snapshot()
	if err != nil {
		return nil, nil, err
	}
	left, err := NewHNSW(h.cfg)
	if err != nil {
		return nil, nil, err
	}
	right, err := NewHNSW(h.cfg)
	if err != nil {
		return nil, nil, err
	}
	if err := left.Restore(snapshot); err != nil {
		return nil, nil, err
	}
	if err := right.Restore(snapshot); err != nil {
		return nil, nil, err
	}
	left.Repair(leftOwns)
	right.Repair(func(id string) bool { return !leftOwns(id) })
	return left, right, nil
}
