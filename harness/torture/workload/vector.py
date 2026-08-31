"""Reference vector workload invariants independent of the Go ANN implementation."""

from harness.torture.checker.invariant import no_duplicate_ids


def run() -> bool:
    return no_duplicate_ids(["a", "b", "c"])
