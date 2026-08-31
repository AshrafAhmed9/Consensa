# Consensa

Consensa is a from-scratch Go vector-storage project exploring durable LSM storage,
deterministic Raft state machines, approximate vector search, and replayable distributed
testing. The build plan is in [PLAN.md](PLAN.md).

## Current runnable surface

The repository currently provides a single-node gRPC process with client-streaming vector
upsert and server-streaming ANN search:

```sh
go run ./cmd/consensa -listen :8080 -dimension 3 -replicas 3
```

The node exposes Prometheus metrics at `http://localhost:9090/metrics` by default; override
the address with `-metrics-listen`.

The protobuf contract is at `api/consensa/v1/consensa.proto`. The in-process gRPC
integration test exercises streamed upsert followed by search without a real socket.

## Verification

```sh
go test ./...
go test -race ./...
go vet ./...
python3 -m pytest harness
```

The project has deterministic simulator, storage recovery, Raft state-machine, range
routing, transaction, lease, and ANN unit tests. `docs/correctness.md` states what those
checks cover and, importantly, what they do not establish yet.

## Status and boundaries

The executable runs an in-memory Raft-replicated ANN graph behind one gRPC listener. It is
not a production multi-process cluster: sockets, durable recovery, range routing, and
transactions are not yet assembled into the public process. ANN search is approximate;
only register/KV histories can meaningfully be checked for linearizability, while search
quality is measured by recall.

No benchmark or availability claim should be inferred from this README. Measurements and
claims are added only after their harnesses exercise the integrated implementation.
