# 003 — a single process is not guaranteed to lead every Raft group it hosts

**Status: found and documented this session, not fixed.** Unlike bugs 001 and 002, this
is a real architectural limitation, not a small code defect with an obvious patch — it is
recorded here, in the same spirit as `docs/adr/007`'s pre-vote gap, so the limitation is a
proven, permanent fact about the system rather than something discovered again later by
surprise.

**Found by:** running `cmd/consensa`'s own `TestConsensaBinaryThreeProcessClusterSurvivesKillAndRestart`
and a standalone `demo.sh` script repeatedly, by hand, dozens of times in a row on one
machine under load. The failure is intermittent — sometimes several consecutive runs pass
cleanly, sometimes several consecutive runs fail the same way — which is exactly why it
had not previously been written up despite the test existing and running in CI: CI's own
runs of this test have occasionally shown the identical failure signature earlier in this
project's history without it being isolated to a specific, named cause before now.

**Symptom:** `ConsensaKV.TransactionalPut` fails with `raft: proposal to non-leader`,
retried against every node, for the test's or demo's entire retry budget (tens of
seconds) — not a transient blip that clears up, but a sustained failure to commit at all
during that window.

**Root cause:** `cmd/consensa` hosts three independent Raft groups per process — the
vector index and the two static KV ranges — sharing one `MultiplexedTransport` listener
but each electing its own leader completely independently. `KVService.TransactionalPut`
never forwards a write to another process (`docs/notes/05-api.md` states this design
choice plainly): it can only commit a transaction spanning both KV ranges if the *same*
process happens to be leading *both* of them at the moment the request arrives.

`hostElectionStagger` (`internal/raft/host.go`) deterministically favors the same
lowest-ranked peer for all three groups *in expectation*, since all three use an
identical peer list and election-tick configuration and tick from the same wall clock.
But each group's pre-vote/vote round trip travels over its own independent sequence of
real network messages, and real message delivery jitter between groups is not
correlated — so the deterministic *bias* is not a deterministic *guarantee*. Under enough
real-world scheduling variance (busy machine, contended CPU, slow disk fsync), the three
groups can settle into a *stable* split — for example, one process leading the vector
index and KV range 1, while a different process leads KV range 2 — with no further
election churn at all. Once that split is stable, `TransactionalPut` cannot succeed
against *any* node until something disturbs it (a restart, a deliberate leadership
transfer, or a lucky future re-election), because no process ever satisfies the "leads
both ranges" precondition on its own.

This is a genuine property of the current design, not a bug in `advanceCommit`, election
safety, or anything this session's joint-consensus work touched — `internal/raft` and
`internal/kv`'s own test suites, which exercise each Raft group in isolation, pass
reliably; only this specific cross-group, single-process-affinity assumption is fragile.

**What was ruled out before landing on this explanation:**
- Two real, independent performance issues were found and fixed while chasing this
  (`raftLog.term`'s O(log length) linear scan made O(1); a periodic
  `AdvanceClosedTimestamp` caller consolidated into the existing tick goroutine instead of
  a second one contending for `Host`'s own mutex) — both are real, valid fixes, kept
  regardless, but neither fully explains this symptom: the identical failure was
  reproduced against a build with neither change present, confirming the underlying flake
  predates this session's work.
- Tick interval, election-settle delays, and retry budget were all independently varied
  (`-tick-interval 20ms` to match the passing test's own configuration; up to 60s retry
  budgets; up to 90 seconds of real elapsed time with a stable, non-churning Raft term
  observed throughout) without reliably preventing the failure — ruling out "just needs
  more time" as the explanation once a stable split has occurred.

**What would actually fix it, not yet attempted:** either (a) a real leadership-affinity
mechanism across co-located Raft groups (e.g., a lightweight leadership-transfer nudge
when a process detects it leads some but not all of its local groups), or (b) forwarding
a `TransactionalPut` to whichever process actually leads each range, which is the
general server-side-forwarding feature `docs/notes/05-api.md` already documents as
out of scope for the whole project, not just this path. Both are real, non-trivial
design work — correctly scoped as future work rather than rushed here, matching how
`docs/adr/007` treats pre-vote's own unfixed gap.
