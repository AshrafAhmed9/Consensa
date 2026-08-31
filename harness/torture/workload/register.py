"""Reference register workload used to exercise the linearizability checker."""

from harness.torture.checker.linearizability import Operation, is_linearizable


def run() -> bool:
    history = [Operation(0, 1, "write", "v", None), Operation(2, 3, "read", None, "v")]
    return is_linearizable(history)
