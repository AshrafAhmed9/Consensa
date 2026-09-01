# 002 — Cluster.Leader() could return a stale leader instead of the real one

**Found by:** running `harness/torture/workload/register.py` against sustained (multi-round)
fault windows for the first time. This bug did not exist as far as any prior test could
see, because it only manifests when an isolation lasts long enough for the reachable
majority to elect a real replacement while the isolated node is still up and still
believes it is leader — a scenario the harness's previous single-round fault model
(`docs/notes/06-torture.md`) could never produce.

**Repro:** 5-node cluster, elect a leader, isolate it for 10 consecutive rounds (well past
every other node's election timeout). The majority correctly elects a new leader at a
higher term partway through. `Cluster.Leader()`, called once per round for the rest of the
run, should consistently report the new leader. It did not — it flipped between the old
and new leader across rounds with no fault or bug in the protocol involved, corrupting the
torture driver's operation history with false write/read ordering and producing
non-linearizable histories in roughly 15% of seeds run under the new fault model, purely
from this — not from any real Raft defect.

**Root cause:** `internal/raft/cluster.go`, `Cluster.Leader()`:

```go
func (c *Cluster) Leader() (NodeID, bool) {
	for id, replica := range c.nodes {
		if replica.(*node).role == Leader {
			return id, true
		}
	}
	return 0, false
}
```

`c.nodes` is a `map[NodeID]Node`. Go map iteration order is unspecified and varies between
calls. During a sustained isolation, two nodes can simultaneously satisfy
`role == Leader`: the isolated node (a "zombie leader" — an expected, tolerated condition,
not itself a bug: it keeps believing it is leader because nothing tells it otherwise) and
the majority's genuine, higher-term replacement. Whichever one the map iteration reached
first was returned, so repeated calls to `Leader()` — with no state change in between —
could return different nodes. Every caller (`ann.ReplicatedIndex`, `kv.multiraft`,
`cmd/torture`) inherited this: a client could be routed to a dead leader in one call and
the live one in the next, with nothing about the underlying cluster having changed.

**Fix:** break the tie deterministically on highest term. The isolated node's term can
never exceed a live quorum's term once a real replacement has been elected, so this can
never surface the stale side after a genuine handover, and it costs nothing when only one
node is a leader (the common case).

```go
func (c *Cluster) Leader() (NodeID, bool) {
	var (
		leader     NodeID
		leaderTerm uint64
		found      bool
	)
	for id, replica := range c.nodes {
		n := replica.(*node)
		if n.role != Leader {
			continue
		}
		if !found || n.term > leaderTerm {
			leader, leaderTerm, found = id, n.term, true
		}
	}
	return leader, found
}
```

**Regression test:** `TestLeaderPrefersHighestTermDuringSustainedIsolation` in
`internal/raft/cluster_test.go` — isolates a 5-node cluster's leader for 10 rounds, asserts
`Leader()` reports the new, higher-term replacement (not the stale one), and asserts ten
repeated calls all agree with each other.

**Why this was invisible until now:** every prior test exercising `Cluster.Leader()`
either never isolated the current leader for long enough to force a real re-election, or
checked the leader immediately after `Tick()`/`Propose()` succeeded — at which point only
one node actually has `role == Leader`, so the map-iteration nondeterminism had nothing to
disagree about. The bug needed a *sustained* isolation of the *actual leader*, specifically
to create the two-simultaneous-leaders window, which nothing before the sustained-window
fault model (`docs/notes/06-torture.md`) could produce.
