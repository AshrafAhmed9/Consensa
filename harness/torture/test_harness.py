from harness.torture.nemesis import schedule


def test_seeded_fault_schedule_replays_exactly():
    assert schedule(7, ["partition", "crash"], 50) == schedule(7, ["partition", "crash"], 50)
