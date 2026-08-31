from harness.torture.checker.invariant import no_duplicate_ids
from harness.torture.nemesis import schedule


def test_seeded_fault_schedule_replays_exactly():
    assert schedule(7, ["partition", "crash"], 50) == schedule(7, ["partition", "crash"], 50)


def test_vector_invariant_rejects_duplicate_visible_ids():
    assert not no_duplicate_ids(["a", "a"])
