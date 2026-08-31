package kv

import (
	"errors"

	"github.com/ashraf/consensa/internal/raft"
)

// Router holds a refreshable descriptor cache. It does not select a leader yet; the
// Raft transport layer will use the returned replica set to do that in a later phase.
type Router struct {
	meta  *Meta
	cache map[string]Descriptor
}

// NewRouter creates a router backed by the supplied metadata catalog.
func NewRouter(meta *Meta) *Router { return &Router{meta: meta, cache: map[string]Descriptor{}} }

// Route resolves key through cache then metadata. Refresh invalidates stale cached ranges.
func (r *Router) Route(key []byte) (Descriptor, error) {
	if d, ok := r.cache[string(key)]; ok && d.Contains(key) {
		return d, nil
	}
	d, e := r.meta.Lookup(key)
	if e == nil {
		r.cache[string(key)] = d
	}
	return d, e
}

// Refresh clears cached metadata after a RangeKeyMismatch response.
func (r *Router) Refresh() { r.cache = map[string]Descriptor{} }

// RoutedKV joins descriptor lookup with MultiRaft command submission. It models the client
// side of range routing: a request first resolves a descriptor, then targets that range's
// elected leader. A real RPC client will retry ErrRangeKeyMismatch after refreshing meta.
type RoutedKV struct {
	router *Router
	raft   *MultiRaft
}

// NewRoutedKV creates a key-addressed facade over hosted Raft ranges.
func NewRoutedKV(router *Router, multiRaft *MultiRaft) *RoutedKV {
	return &RoutedKV{router: router, raft: multiRaft}
}

// Put routes and replicates a key/value mutation. The descriptor check before and after
// lookup prevents a caller from accidentally using an invalidated cached range identity.
func (k *RoutedKV) Put(key, value []byte) error {
	return k.retryRoute(key, func(descriptor Descriptor) error {
		return k.raft.Put(descriptor.ID, key, value)
	})
}

// Delete routes and replicates a key removal.
func (k *RoutedKV) Delete(key []byte) error {
	return k.retryRoute(key, func(descriptor Descriptor) error {
		return k.raft.Delete(descriptor.ID, key)
	})
}

// Get routes to the currently elected leader. Follower reads require a lease/read-index
// proof and are deliberately deferred to the lease phase rather than silently serving
// possibly stale data here.
func (k *RoutedKV) Get(key []byte) ([]byte, error) {
	var value []byte
	err := k.retryRoute(key, func(descriptor Descriptor) error {
		leader, ok := k.raft.Leader(descriptor.ID)
		if !ok {
			return ErrRangeKeyMismatch
		}
		var err error
		value, err = k.raft.Get(descriptor.ID, raft.NodeID(leader), key)
		return err
	})
	return value, err
}

func (k *RoutedKV) retryRoute(key []byte, request func(Descriptor) error) error {
	for attempt := 0; attempt < 2; attempt++ {
		descriptor, err := k.router.Route(key)
		if err != nil {
			return err
		}
		if !descriptor.Contains(key) {
			err = ErrRangeKeyMismatch
		} else {
			err = request(descriptor)
		}
		if !errors.Is(err, ErrRangeKeyMismatch) {
			return err
		}
		// A range leader owns the authoritative descriptor. Its mismatch response is
		// the proof that this cached route is stale, so retry exactly once after a
		// metadata refresh rather than hiding persistent topology errors in a loop.
		k.router.Refresh()
	}
	return ErrRangeKeyMismatch
}
