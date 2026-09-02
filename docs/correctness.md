# Correctness status

Consensa uses deterministic schedules so a failing seed can be recorded and replayed.
The register checker tests small histories for linearizability. The ANN checker measures
recall separately: approximate search is not, and must not be described as, linearizable.

Current checks cover unit-level Raft election/replication properties, WAL recovery,
canonical ANN snapshots, and deterministic harness schedules. They do not yet prove an
end-to-end distributed cluster under the seeded fault-injection matrix the plan calls
for; that claim will be added only after the Python torture harness drives real
replicated API workloads across the full fault matrix (`PLAN.md` Phase 6), not before.

**A real finding: the CI pipeline itself had never once run successfully.**
`.github/workflows/ci.yml` pinned `golangci-lint-action` to `v2.8.0`, built with Go 1.25;
`go.mod` has said `go 1.26.0` since the very first Go commit of this project
(`de24b118`), and golangci-lint refuses to lint a module targeting a newer Go version
than it was itself built with. Every push since the pivot to Go -- every commit this
session and every one before it -- was red on GitHub Actions before the lint step ever
ran a single check, discovered only by actually running `gh run list` and reading the
failure, not by re-reading source. This means every "tests pass" claim in this document
and elsewhere was true of what was run *locally*, but had never once been independently
confirmed by the CI pipeline a real reviewer would actually look at. Fixed by pinning
`golangci-lint-action` to `v2.13.2` (verified to exist as a real published release, and
built with a Go version new enough to lint `go 1.26.0`) and fixing the three real
`revive` `exported`-rule violations it had been silently never catching (missing doc
comments on `raft.Role`'s and `raft.MessageType`'s constant blocks, and
`txn.Status`'s). Checked against the actual per-step run logs (`gh run view --json jobs`), not assumed:
`go vet ./...` genuinely ran and passed on every push, but `go test -race ./...` and
`python -m pytest harness/torture` were never reached at all -- GitHub Actions stops a
job's remaining steps after one fails, so both were reported `skipped` on every single
run since the Go pivot. The claims in this document about `-race` and the torture
harness passing are backed by this session's own local runs (and, per each commit's own
message, verified before every push), not by CI ever having confirmed them
independently -- that gap is now closed going forward, not backfilled for the commits
already on `main`.

**The register workload is now real, and its current limit is precisely known.** Before
this session, `register.run()` checked a fixed, hand-written history — the seed and
`--nemesis` flags existed but had no effect on the outcome. `cmd/torture` now drives a
real `raft.Cluster` under the seed's actual fault schedule and exports a real
client-observable history for `is_linearizable` to check. This is a real limitation of
the fault *model*, not evidence the checker is decorative — the checker itself is
independently proven to reject a bad history in
`harness/torture/checker/test_linearizability.py`, and the Go-driven history correctly
reflects genuine fault effects (a proposal to an isolated "zombie leader" is correctly
never recorded as a successful write; reads during isolation correctly return stale, not
phantom, values).

**A real bug was found and fixed in this session, by the harness — not the workload it
was built to test.** Making the fault schedule generate sustained multi-round isolation
windows (instead of single-round events) immediately exposed a genuine correctness bug in
`Cluster.Leader()`: it picked among nodes with `role == Leader` using Go's undefined map
iteration order, so during a sustained isolation, a stale "zombie leader" and the real,
higher-term replacement elected by the majority could both be returned inconsistently
across rounds — corrupting the driver's history with false non-linearizability, at
roughly a 15% rate per seed. Fixed by breaking the tie on highest term, which is always
the node a real client would actually observe as current. This is a permanent fix to a
widely shared method (`ann.ReplicatedIndex`, `kv.multiraft`, and this harness all call
`Cluster.Leader()`), not a narrow harness-only patch. With it fixed, the harness runs
clean across 200+ seeds against the correct implementation.

**The Figure-8 half of this phase's DoD is now closed, by a scripted test rather than
the seeded harness.** Weakening the commit rule and running 500+ torture seeds under
sustained isolation windows never caught it — a uniformly random fault target rarely
produces the exact log-divergence-then-reconvergence pattern Figure 8 needs.
`TestFigure8UnsafeCommitWouldBeOverwritten` (`internal/raft/raft_test.go`) closes it
directly: it drives the paper's actual 5-server scenario through real `Step()` calls —
real vote-freshness checks, real `AppendEntries` accept/reject/truncate, and the real
`advanceCommit` term guard — and shows an entry replicated to a genuine 3-of-5 majority
still gets silently overwritten by a later leader, because it was never anchored by an
entry from that leader's own term. Verified as a real, discriminating check, not a
vacuous one: it passes against the correct implementation and fails
(`committed a prior-term entry despite the Figure-8 guard: committed=2`) when the same
guard this session kept weakening in earlier torture-harness experiments is weakened
again. Only the scenario's starting log/term states are hand-set (matching the
existing `TestFigure8CommitRule`/`TestDelayedPreVoteResponseCannotRestartLeader`
precedent in the same file); every safety-relevant decision after that runs through
unmodified production code.

**The pre-vote half of that DoD is retired, not achieved, and the reason is a real
finding, not a harness shortcoming.** Testing an *asymmetric* partition — one follower
cut off from the leader only, fully connected to every other follower — shows the
disruptor wins real elections and repeatedly displaces the healthy leader **even with
pre-vote correctly implemented**. This implementation's pre-vote, like the base Raft
paper, has no notion of "reject a vote if I currently have a healthy, reachable leader"
(etcd calls this `CheckQuorum`; it is not implemented here) — it only stops a
*reconnecting* node's inflated term from causing harm, which is a different bug class
from a *persistently* asymmetric partition. No fault schedule the torture harness can
generate (full bidirectional isolation, of any duration) produces this scenario, and the
scenario that does produce it doesn't distinguish a weakened pre-vote from a correct one
anyway — so this was never achievable by hardening the fault model, seeded or not.
`docs/adr/007-prevote-does-not-cover-persistent-asymmetric-partitions.md` has the full
account and the decision to leave it unfixed for now; the gap is proven permanently by
`TestAsymmetricPartitionDisruptsHealthyLeader` in `internal/raft/cluster_test.go`, not
just described. `docs/notes/06-torture.md` has the trace-level path that led here.

**The vector workload is now real too, closing the last "checks a fixed, hardcoded
value" gap in the torture harness.** `cmd/vectortorture` drives real per-replica
`internal/ann.HNSW` graphs through the same fault-injectable `raft.Cluster` path
`cmd/torture` uses, checking that every replica which applied the same number of
mutations has a byte-identical graph and no replica ever contains a duplicate ID.
Verified clean across 60 seeds under `partition`/`crash` faults at 5 nodes. It does not
check recall — see `docs/notes/06-torture.md`'s vector-workload section for why that
would conflate two different claims this document is careful to keep separate.

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
  query afterward. `ReplicatedIndex` is explicitly in-memory only; this is the first proof
  that durability actually works end to end, not just that the storage engine passes its
  own unit tests in isolation.
- **The actual shipped binary, not just the library code.** `cmd/consensa/main.go` now
  runs the `DurableNode` path — real TCP Raft, real storage, one process per replica — and
  two things confirm the binary itself (flag parsing, gRPC wiring, the tick loop) is
  correct, not only the packages underneath it: `cmd/consensa/main_e2e_test.go` builds the
  binary and runs three real OS processes, upserts through gRPC, kills one process's real
  socket, confirms the surviving majority keeps accepting writes, restarts a fourth process
  against the killed node's data directory, and confirms it recovers over gRPC before
  talking to any peer. Separately, `deploy/docker-compose.yml` — which, before this
  session, referenced a `Dockerfile` that did not exist and therefore could never have
  built — was run by hand as three real containers on a real Docker network: a real
  `docker compose kill consensa3` followed by `docker compose up -d consensa3` recovered
  correctly from the container's persisted volume, verified with a real gRPC client
  against `localhost:8083`, not simulated.

What remains unproven, stated plainly: multi-range sharding, transactions, and the Python
torture harness have not been exercised against the durable path under sustained chaos.
Writes are not forwarded to the leader server-side — a client that reaches a non-leader
replica gets a real `"not leader"` error and must retry elsewhere itself (see
`docs/notes/05-api.md` for the full accounting). `BatchGet` reads exact payloads from a
durable index when the backend supplies them, so it survives service-process recreation;
the old process-local map is only a compatibility fallback for minimal in-memory
adapters. Do not read the tests and manual verification above as proof of the full
distributed-systems claim in the README; they are proof that the actual shipped artifact
— not just its component packages in isolation — does what the README claims for the
pieces it covers.

## Later update: sharding, transactions, membership changes, and the read-path ladder

Everything below was closed in later sessions, after the account above was written. Each
item names its own test and the doc where the full account (including what it deliberately
does *not* prove) lives — this section is a current index, not a replacement for those.

- **Cross-range transactions are real and reachable over gRPC.** `internal/txn`'s 2PC
  coordinator (HLC timestamps, write intents, a transaction record as the sole atomic
  commit point) runs unmodified over both an in-memory `Store` and a Raft-backed
  `DurableStore`. `TestCoordinatorCommitsAcrossRealRaftRanges` proves a transaction
  committing across two genuinely separate 3-node `kv.DurableRange` groups;
  `TestTransactionalPutCommitsAcrossRealRangesOverGRPC` proves the same thing driven by a
  real network client through `ConsensaKV.TransactionalPut`. See
  `docs/adr/008-wire-txn-onto-durable-ranges.md` and `docs/notes/08-transactions.md`.
- **Live range splits preserve correctness on both planes.** `TestLiveSplitPreservesSearchCorrectness`
  (vector) and `TestLiveSplitPreservesKVCorrectness` (KV) each split a real 3-node group
  into two fresh 3-node child groups by rebuilding from the parent's actual applied data,
  and prove no record lost or duplicated and no cross-boundary leakage in either child.
  `ShouldSplit`/`DurableRange.MaybeSplitKey` close the split-*trigger decision* (a real
  size threshold picking the median key of the range's actual data,
  `TestMaybeSplitKeyDrivesARealLiveSplit`) — not automatic execution or live traffic
  cutover, which remain open. See `docs/notes/12-split-repair.md`.
- **Multi-range outbound connections are pooled.** Ranges on one node sharing a
  destination reuse one persistent TCP connection instead of dialing per message
  (`TestMultiplexedTransportPoolsOutboundConnections`). Making the receiving side read
  many frames per connection surfaced and fixed two real bugs: a `bufio.Reader`
  re-wrapped on every read that silently dropped bytes, and cross-range head-of-line
  blocking from dispatching a received frame synchronously off the shared connection's
  one read goroutine. See `docs/notes/07-multiraft.md`.
- **Learners and full joint-consensus membership changes are wired into the live quorum
  path.** `Config.Learners` marks replicating-but-non-voting peers, proven not to count
  toward commit quorum even when they'd otherwise form a literal majority
  (`TestCommitNeverAdvancesOnLearnerAcksAlone`). On top of that, `ProposeConfChange`
  drives Raft's real two-phase joint-consensus protocol: a config-change entry takes
  effect the moment it's *appended* to a replica's own log, not once committed (the
  specific rule that makes the protocol safe); during the joint phase, both elections and
  commits require a majority of the old configuration *and* a majority of the new one
  separately, closing the exact disjoint-majority failure mode joint consensus exists to
  prevent (`TestJointConfigRejectsDisjointMajorities`); the transition finalizes
  automatically once the joint entry commits, and a leader that removes itself steps down
  (`TestJointTransitionCompletesAndFinalizesAutomatically`,
  `TestRemovedLeaderStepsDown`). Config state survives snapshots and leader crashes
  mid-transition. Provisioning a genuinely new, previously-unknown process is now also
  proven, not just reconfiguring already-known peers: `Node.AddKnownPeer`/`Host.AddPeer`
  (a matching pair of local, per-replica primitives -- one extends `ProposeConfChange`'s
  eligibility check, the other registers the new process's real transport address) let a
  fourth real `*Host`, started only after the other three already elected a leader and
  committed real entries, join as a learner over a real TCP connection and get promoted
  to a full voter (`TestBrandNewProcessJoinsLiveGroupAsLearnerThenVoter`). Still unbuilt:
  no authenticated admin RPC surface triggers any of this over the network, and updating
  client routing after a membership change is separate work. See `docs/adr/010-learners.md`.
  A real performance regression was found and fixed while closing this: recomputing
  membership from the full log on every heartbeat and every proposed write (not just
  actual config changes) was cheap in unit tests but measurably destabilized leadership
  under the sustained write load of `cmd/consensa`'s own three-process end-to-end test in
  CI — caught as an actual CI failure (`raft: proposal to non-leader`), not by inspection,
  and fixed by only recomputing when a log change could actually affect membership.
- **The read-path ladder closes end to end.** Lease grants, closed-timestamp advancement,
  and `DurableRange.FollowerRead` are real Raft-replicated operations proven against a
  real 3-node group: `TestFollowerReadServesOnceLeasedAndClosed` checks every rejection
  path (no lease; lease but no closed timestamp; a replica that isn't the lease's
  intended holder) as well as a follower correctly answering a read from local storage
  alone. `cmd/consensa` now advances the closed timestamp and grants/renews leases on a
  real interval automatically (`maintainLeases`, `advanceClosedTimestamps`). Still open:
  lease revocation on leadership change, and an RPC surface exposing `FollowerRead` to a
  network client. See `docs/notes/09-leases.md` and ADR-009.
- **A reproduced write-skew anomaly is actually prevented, not just documented — and a
  pushed transaction can now read-refresh and commit instead of always aborting.**
  `Store.WriteIntent` and `DurableStore.WriteIntent` both reject a write whose timestamp
  collides with an already-recorded read on the same key — the classic two-doctors-on-call
  anomaly is reproduced end to end and the specific write that would complete it is
  rejected (`TestWriteIntentRejectsWriteSkew`, `TestDurableStoreRejectsWriteSkew`).
  `Coordinator.Prepare` no longer treats that rejection as an automatic abort: it
  computes the pushed timestamp every conflicting intent needs, asks each participant to
  validate the transaction's own prior reads are still current there
  (`Store.RefreshReads`), and only aborts if that validation actually fails
  (`TestPrepareRefreshesInsteadOfAbortingWhenPriorReadsStillHold`,
  `TestPrepareAbortsWhenRefreshFindsAStaleRead`). Proven for both the in-memory `Store`
  and the real, Raft-replicated `DurableStore`
  (`TestDurableStorePrepareRefreshesInsteadOfAborting`,
  `TestDurableStorePrepareAbortsWhenRefreshFindsAStaleRead`), the latter durably indexing
  each key's last-committed-write timestamp the same way its existing read/intent
  indexes work. See `docs/notes/14-serializable.md`.
- **TLA+ now covers joint-consensus quorum intersection and recursive range splitting**,
  each checked by TLC with a required negative-control variant that must fail — proving
  the checker itself is discriminating, not just that the correct model happens to pass.
  See `specs/README.md`.
