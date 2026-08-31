# ADR 001: Go is the data-plane language

## Decision

Consensa's storage, consensus, and index code is written in Go. Python remains the client
and verification language.

## Context and consequences

The earlier Python direction was discarded. Go makes a small single-binary node practical
and is the language used by many systems roles this project is intended to demonstrate.
More importantly, the planned Raft implementation can be a pure state machine whose time
and I/O are explicit; Go's preemptive scheduler then cannot leak into an algorithm test.

This costs iteration speed and discards earlier Python storage work. That is accepted in
exchange for a clearer production/data-plane boundary. Python is still the independent
checker, where NumPy and pytest are useful rather than incidental.
