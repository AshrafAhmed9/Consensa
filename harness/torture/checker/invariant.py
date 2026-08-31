"""Small reusable state invariants for deterministic workload tests."""


def no_duplicate_ids(ids: list[str]) -> bool:
    """A committed vector set cannot contain two visible versions of one ID."""
    return len(ids) == len(set(ids))


def non_negative_recall(value: float) -> bool:
    return 0.0 <= value <= 1.0
