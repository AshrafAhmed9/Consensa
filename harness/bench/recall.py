"""Exact nearest-neighbour search and the recall measure, used to judge the Go index."""

import numpy as np


def top_k(vectors: np.ndarray, query: np.ndarray, k: int) -> list[int]:
    """Returns the indices of the k vectors nearest to query, by checking every vector."""
    distances = np.sum((vectors - query) ** 2, axis=1)
    return np.argsort(distances, kind="stable")[:k].tolist()


def recall_at_k(expected: list[int], actual: list[int]) -> float:
    """Returns the fraction of the true top-k items that the approximate index also returned."""
    if not expected:
        return 1.0
    return len(set(expected) & set(actual)) / len(expected)
