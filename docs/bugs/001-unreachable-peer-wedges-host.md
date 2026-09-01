# 001 — a single unreachable peer wedges the whole Raft host

**Found by:** `internal/raft/host_test.go`, `TestHostTCPClusterSurvivesLeaderFailure`, added
while extending the real-TCP coverage of `Host`/`TCPTransport`. A real TCP test already
existed (`TestHostsElectAndReplicateOverTCP`, a 2-node election-and-replicate happy path
where both nodes stayed up for the whole test) and was passing — so the transport itself
was not untested, only the specific scenario that mattered: what happens when a peer
becomes unreachable mid-cluster. That scenario has no analogue in
`internal/raft/cluster_test.go`, which drives the pure protocol via
`Cluster.Tick`/`DeliverFiltered` — direct function calls that cannot fail the way a TCP
dial can, so a "message send fails" code path is structurally unreachable through that
harness regardless of what scenario it's given.

**Repro:** start a 3-node `Host` cluster on real TCP, elect a leader, propose an entry,
close the leader's transport and storage (simulating a crash), then propose again against
the two survivors. Expected: the survivors elect a new leader and keep committing, since
2-of-3 is a quorum. Actual: neither survivor ever became leader within a 5-second deadline.

**Root cause:** `Host.driveLocked` (`internal/raft/host.go`) looped over `ready.Messages`
and returned immediately on the first `transport.Send` error:

```go
for _, message := range ready.Messages {
    if err := h.transport.Send(message); err != nil {
        return err
    }
}
```

`TCPTransport.Send` dials the target with a 1-second timeout, so a message addressed to
the now-dead node reliably fails. Returning early meant:

1. Messages to the *other*, live peer — later in the same `Ready.Messages` slice — were
   never sent.
2. `ready.CommittedEntries` were never applied.
3. `h.node.Advance()` was never called, so the node's internal unstable/applied state
   never cleared. The next `Tick()` re-emitted the identical `Ready`, including the same
   doomed message to the dead peer — so the host repeated this failure forever instead of
   making progress.

The bug is a category error: `Send` failing to one peer is an ordinary, expected condition
under Raft's own fault model (a *minority* of nodes may be down and the protocol must
still make progress), but the code treated it as fatal to the entire `Ready` cycle,
identically to a `Persist` failure — which genuinely is fatal, since losing durable state
is not recoverable the way a dropped network message is.

**Fix:** stop propagating per-message send errors. Attempt every message in `Messages`
regardless of earlier failures, and always reach `CommittedEntries` and `Advance()`.
`Persist` errors are unchanged and still abort the cycle — that boundary is correct and
was not the bug.

```go
// A send failure to one peer must not block progress toward the others: Raft is
// designed to tolerate a minority of unreachable nodes, and any message dropped here
// gets naturally retried on the next heartbeat once the node is Ready() again.
for _, message := range ready.Messages {
    _ = h.transport.Send(message)
}
```

**Regression test:** `TestHostTCPClusterSurvivesLeaderFailure` in
`internal/raft/host_test.go` — kills the current leader's real TCP transport and storage
mid-test and asserts the surviving majority elects a new leader and continues committing
within a bounded deadline. It replaces the original single 2-node happy-path test with a
3-node harness (`TestHostTCPClusterElectsAndReplicates` covers the same election-and-
replicate ground the original test did, plus a third node) so both the healthy path and
the failure path now share one set of test helpers.

**Why this was invisible until now:** the one prior real-TCP test never removed a node
from the cluster, so it never called `Send` against an address nothing was listening on.
`Cluster` (the in-memory harness used by `internal/ann/replicated.go` and every other
existing integration test) delivers messages via direct function calls that cannot fail
the way a TCP dial can, so it could not have caught this regardless of what scenario it
was given. `Host`/`TCPTransport` is also not yet what `cmd/consensa/main.go` runs in
production — that binary currently assembles the in-memory `Cluster` path only — so this
bug would have shipped silently the first time a real multi-process deployment actually
lost a node, which is exactly the situation the whole component exists to survive.
