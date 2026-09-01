# Correctness status

Consensa uses deterministic schedules so a failing seed can be recorded and replayed.
The register checker tests small histories for linearizability. The ANN checker measures
recall separately: approximate search is not, and must not be described as, linearizable.

Current checks cover unit-level Raft election/replication properties, WAL recovery,
canonical ANN snapshots, and deterministic harness schedules. They do not yet prove an
end-to-end distributed cluster under the seeded fault-injection matrix the plan calls
for; that claim will be added only after the Python torture harness drives real
replicated API workloads across the full fault matrix (`PLAN.md` Phase 6), not before.

Two things beyond unit tests are now proven, both by dedicated integration tests:

- **Real transport, real failover.** `internal/raft/host_test.go` runs three `Host`
  replicas over real loopback TCP, confirms they elect a leader and replicate a proposal's
  exact bytes to every follower, then closes the current leader's socket mid-test and
  confirms the surviving 2-of-3 majority elects a new leader and keeps committing. A
  simpler 2-node happy-path TCP test already existed and passed; extending it to a
  deliberate node failure found a real bug — a single unreachable peer could wedge the
  entire host indefinitely (see `docs/bugs/001-unreachable-peer-wedges-host.md`) — because
  the happy path never called `Send` against an address nothing was listening on, and the
  other harness (`Cluster`) delivers messages via direct function calls that cannot fail
  the way a TCP dial can, so it could not have caught this under any scenario.
- **Real durability.** `internal/ann/durable_test.go` proves a killed-and-restarted vector
  index replica (`DurableNode`, backed by a real `raft.Host` and a real `storage.Engine`)
  recovers its entire HNSW graph from its own on-disk Raft log alone — before exchanging a
  single message with either surviving peer — and answers a correct nearest-neighbour
  query afterward. `ReplicatedIndex`, the composition every earlier test and
  `cmd/consensa/main.go` use, is explicitly in-memory only; this is the first proof that
  durability actually works end to end, not just that the storage engine passes its own
  unit tests in isolation.

What remains unproven, stated plainly: `cmd/consensa/main.go` does not yet run the durable
`Host`/`DurableNode` path — it still assembles the in-memory `Cluster`/`ReplicatedIndex`
path for the gRPC demo. Multi-range sharding, transactions, and the Python torture harness
have not been exercised against either path under sustained chaos. Do not read the two
integration tests above as proof of the full distributed-systems claim in the README;
they are proof that the two most safety-critical pieces of previously-untested machinery
(the real transport, and real recovery from disk) actually do what they claim.
