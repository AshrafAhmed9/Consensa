"""Small reusable state invariants for deterministic workload tests."""


def no_duplicate_ids(ids: list[str]) -> bool:
    """A committed vector set cannot contain two visible versions of one ID."""
    return len(ids) == len(set(ids))


def non_negative_recall(value: float) -> bool:
    return 0.0 <= value <= 1.0


def doctors_always_on_call(violations: list[dict]) -> bool:
    """The doctors-on-call write-skew invariant: at least one doctor must be on call after
    every committed transaction. cmd/doctortorture checks real replicated state after each
    commit itself (it has to -- only it knows when a commit actually happened) and reports
    any round where every doctor ended up off call as a violation; this function's whole job
    is just "were there any," so a passing run's meaning is legible at the call site in
    workload/doctors.py rather than re-deriving it from a raw report dict there.
    """
    return not violations
