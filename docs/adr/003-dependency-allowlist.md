# ADR 003: Keep the dependency surface closed

## Decision

The Go data plane uses the standard library plus gRPC/protobuf for the eventual public
API and Prometheus for metrics. The Python side uses NumPy, grpcio, Hypothesis, and pytest.
Anything else needs an ADR.

## Rationale

Raft, skiplist, LSM, transport framing, vector search, and distance kernels are the
subjects being learned and demonstrated; importing them would hide the important design
choices. gRPC and Prometheus are admitted because their protocol and exposition plumbing
does not advance that goal. gRPC is client-facing only—the simulated peer transport must
remain visible to the scheduler.
