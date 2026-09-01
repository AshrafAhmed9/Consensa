# Phase 5: gRPC API

## Why does this exist?

The gRPC surface lets bulk-ingest and retrieval clients use Consensa without depending on
the internal storage, Raft, or index packages.

## How does it work?

Upsert is client-streaming because ingest is batch-oriented. Search is server-streaming so
large result sets can flow incrementally. The service validates IDs, dimensions, and `k`
at the boundary, returning gRPC status errors instead of panics.

## What alternatives existed?

HTTP JSON is easier to inspect but less natural for high-volume typed vector payloads.
Hand-written RPC framing would duplicate commodity protocol work and distract from the
data-plane implementation.

## What tradeoff was made?

The `Index` interface (`server.Index`) is deliberately narrow -- Insert/Delete/Search/
Validate -- so the same `Service` composes with three different backends without knowing
which one it has: a bare `*ann.HNSW` (single-node, in-process, what the earliest tests
used), `*ann.ReplicatedIndex` (Raft-ordered but in-memory only), or `*ann.DurableNode`
(Raft-ordered over real TCP, backed by a real on-disk storage engine -- what
`cmd/consensa/main.go` actually runs now). The public contract did not need to change
across any of those three; that stability is the payoff of keeping the interface narrow.

## What can fail?

**Writes do not forward to the leader on a client's behalf.** `DurableNode.Insert`/
`Delete` only succeed when the replica *this specific gRPC connection* landed on happens
to be the current Raft leader; otherwise the RPC returns the real `"raft: proposal to
non-leader"` error, and the client is expected to retry against a different node in the
deployment's `--peers` list (see `cmd/consensa/main_e2e_test.go`'s
`upsertUntilAccepted`, and the equivalent behavior in `internal/raft/host_test.go`'s
`proposeToLeader`, for the pattern). This is an honest client-side burden, not a bug --
but a production deployment would eventually want either server-side forwarding to the
leader or a client library that hides the retry, and neither exists yet.

Reads (`Search`/`Validate`/`BatchGet`) do not have this restriction: any replica answers
them locally once a mutation is applied, since Raft-ordered mutation makes every replica's
graph equal by construction (see `internal/ann/doc.go`).

`BatchGet` still reads from `Service.vectors`, a plain in-memory map local to whichever
process answers the RPC -- it is populated only by writes that landed on *that* process
(i.e., only ever the leader, historically), not reconstructed from the replicated index.
A `BatchGet` sent to a follower that has never been leader will not see IDs another
replica accepted. This is a real, currently-undocumented-elsewhere gap; fixing it means
sourcing `BatchGet` from the index itself rather than the service's local bookkeeping map.

The actual multi-process, real-TCP, real-storage deployment path is now proven twice: once
via `cmd/consensa/main_e2e_test.go` (three real OS processes, kill and restart one, verify
recovery from disk over real gRPC), and once by hand via `deploy/docker-compose.yml`
(three real containers on a real Docker network, `docker compose kill consensa3` +
`up -d consensa3` recovers correctly from the container's persisted volume). Before this
session, `docker-compose.yml` referenced a `Dockerfile` that did not exist at all, and
`cmd/consensa/main.go` only ever ran the in-memory `ReplicatedIndex` demo path -- so
neither claim in this file was actually checkable until now.
