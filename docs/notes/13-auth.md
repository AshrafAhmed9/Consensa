# Phase 13: a shared-secret auth layer

## Why does this exist?

Every RPC in this project (`Consensa`, `ConsensaKV`, `ConsensaAdmin`) was, honestly,
documented as unauthenticated for most of this project's life -- `api/consensa/v1/consensa.proto`
and `docs/adr/010-learners.md` both said so plainly rather than leaving the gap implied.
That was fine for local demos and CI, but `ConsensaAdmin` in particular can add, promote,
or remove Raft replicas: exposing that with zero access control on a real network is a
real, stated risk, not a theoretical one. This closes the gap with the smallest mechanism
that is still a genuine access control, not a full identity system.

## How does it work?

`internal/auth.TokenAuth` wraps up to two operator-configured shared secrets: a
data-plane token (`--auth-token`, gating `Consensa` and `ConsensaKV`) and an admin token
(`--admin-auth-token`, gating `ConsensaAdmin`). It installs as a
`grpc.UnaryServerInterceptor`/`StreamServerInterceptor` pair on the single `grpc.Server`
`cmd/consensa/main.go` already builds -- one interceptor pair covers all three registered
services, since they share one listener and one process, deciding which token a call
needs purely from `grpc.UnaryServerInfo`/`StreamServerInfo`'s own `FullMethod` (e.g.
`/consensa.v1.ConsensaAdmin/AddReplica`), with no per-service wiring needed anywhere else.
If `--admin-auth-token` is left unset it falls back to `--auth-token`, so a deployment
that only ever wanted one secret protecting everything (this package's original shape)
still gets that with a single flag. Every RPC's incoming metadata is checked for an
`authorization: Bearer <token>` value, compared against the required secret with
`crypto/subtle.ConstantTimeCompare` rather than `==`, closing the standard timing side
channel a naive string comparison leaks (a network attacker who can measure response
latency precisely enough can otherwise recover a secret one byte at a time by watching
which candidate byte takes marginally longer to mismatch).

The client side, `internal/auth.BearerCredentials`, implements
`credentials.PerRPCCredentials` so a caller attaches its token once at dial time
(`grpc.WithPerRPCCredentials(...)`) instead of threading it through every individual call
site. `cmd/democlient` was updated to take its own `--auth-token` flag as a real usage
example, and `cmd/consensa/main.go`'s own gRPC server construction is the reference
server-side wiring.

An empty token disables the whole layer (`TokenAuth.Enabled() == false`), which is not
merely "authentication that always succeeds" but an actual no-op fast path: the
interceptor returns immediately without inspecting metadata at all. This is what keeps
every existing deployment, e2e test, and demo client -- none of which ever learned about
auth -- working unmodified.

## What alternatives existed?

A full identity system (per-user accounts, scoped permissions, JWTs with expiry and
rotation) would be the production answer, but it is a large amount of code and design
surface for a portfolio project whose point is the distributed-systems core, not an
identity provider -- exactly the kind of complexity `PLAN.md`'s own portfolio-engineering
guidance says to avoid unless it demonstrates something the project is actually about.
mTLS (client certificates) was another option, and is arguably the more idiomatic gRPC
answer, but it entangles auth with transport security in a way that makes it harder to
reason about and test independently, and this project has no TLS story anywhere yet to
build on. A shared-secret bearer token is the simplest mechanism that is still a *real*
access control (not a placeholder), independently testable without any TLS setup, and a
natural stepping stone to mTLS or a real identity provider later if this project's scope
ever grows to need one.

## What tradeoff was made?

Scoping stops at the service boundary, not the method. `--admin-auth-token`, separate
from `--auth-token`, means a data-plane credential cannot call `ConsensaAdmin`'s
membership-changing RPCs -- but within `ConsensaAdmin`, one valid admin token authorizes
both `AddReplica` and `PromoteReplica` equally; within the data plane, one valid token
authorizes `Upsert`, `Search`, `Delete`, `BatchGet`, `Status`, and `TransactionalPut`
equally. A real production deployment wanting per-method permissions, or per-user
identity distinguishing WHO called something (not just whether the call was authorized
at all), would need a real identity system this package deliberately is not.
`RequireTransportSecurity` deliberately returns `false`: this
project's entire deployment story (docker-compose, the e2e tests, the demo script) runs
over plaintext TCP, so requiring TLS here would make the credential simply unusable rather
than more secure. The tradeoff is stated in the package doc comment itself, not hidden:
this stops an unauthenticated caller from reaching the API; it does not stop network
eavesdropping, which needs TLS layered on separately.

## What can fail?

A client that forgets to configure a matching token gets a clear `Unauthenticated` gRPC
status, not a silent failure or a hang -- `TestConsensaBinaryEnforcesAuthTokenWhenConfigured`
proves this against the real binary. A deployment that sets `--auth-token` on some nodes
but not others in the same `--peers` list would produce a genuinely confusing situation
(some nodes reject the same client credentials others accept) -- this is an operational
misconfiguration this layer does not detect or warn about, since every node's flag is
independent and there is no cluster-wide config validation anywhere in this project. The
token itself, if leaked (logged, committed to source control, exposed via a process
listing), grants full access with no way to revoke just that one credential short of
restarting every node with a new `--auth-token` -- there is no per-token revocation,
because there is no concept of more than one token at all.
