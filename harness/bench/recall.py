"""Deterministic brute-force recall helpers used by ANN benchmark scripts."""

import numpy as np


def top_k(vectors: np.ndarray, query: np.ndarray, k: int) -> list[int]:
    """Returns exact L2-nearest vector indices; this is intentionally independent of Go."""
    distances = np.sum((vectors - query) ** 2, axis=1)
    return np.argsort(distances, kind="stable")[:k].tolist()


def recall_at_k(expected: list[int], actual: list[int]) -> float:
    """Measures the fraction of exact top-k items returned by an approximate index."""
    if not expected:
        return 1.0
    return len(set(expected) & set(actual)) / len(expected)
