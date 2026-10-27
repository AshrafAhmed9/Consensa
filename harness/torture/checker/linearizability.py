"""A small linearizability checker for a single register (one value that is read and written).

A history is linearizable if its operations can be put in one order such that:
  - every read returns the value of the latest write before it in that order, and
  - an operation that finished before another began stays before it in that order.
Operations that overlap in time may go in either order.

The search tries every valid order, so it is only meant for short histories.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Operation:
    invocation: int      # time the operation started
    response: int        # time the operation finished
    kind: str            # "write" or "read"
    value: str | None    # the value written (writes only)
    result: str | None   # the value returned (reads only); None means "nothing written yet"


def is_linearizable(history: list[Operation]) -> bool:
    """Returns True if some valid order of the operations explains every read."""

    def search(done: frozenset[int], value: str | None) -> bool:
        # done: indexes of operations already placed. value: the register's current value.
        if len(done) == len(history):
            return True
        for i, op in enumerate(history):
            if i in done:
                continue
            # op cannot go next if some unplaced operation finished before op started.
            if any(other.response < op.invocation and j not in done for j, other in enumerate(history)):
                continue
            if op.kind == "write":
                if search(done | {i}, op.value):
                    return True
            elif op.kind == "read" and op.result == value and search(done | {i}, value):
                return True
        return False

    return search(frozenset(), None)
