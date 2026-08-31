from harness.bench.recall import recall_at_k


def within_band(expected: list[int], actual: list[int], minimum: float) -> bool:
    """Checks measured ANN recall against a deliberately explicit tolerance band."""
    return recall_at_k(expected, actual) >= minimum
