"""Small Wing-Gong-style checker for a single linearizable register history."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Operation:
    invocation: int
    response: int
    kind: str
    value: str | None
    result: str | None


def is_linearizable(history: list[Operation]) -> bool:
    """Returns whether a sequential register order can respect real-time precedence."""
    def search(done: frozenset[int], value: str | None) -> bool:
        if len(done) == len(history):
            return True
        for i, op in enumerate(history):
            if i in done:
                continue
            if any(other.response < op.invocation and j not in done for j, other in enumerate(history)):
                continue
            if op.kind == "write":
                if search(done | {i}, op.value):
                    return True
            elif op.kind == "read" and op.result == value and search(done | {i}, value):
                return True
        return False
    return search(frozenset(), None)
