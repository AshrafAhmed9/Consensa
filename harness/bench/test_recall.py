import numpy as np

from recall import recall_at_k, top_k


def test_ground_truth_and_recall():
    vectors = np.array([[0, 0], [2, 2], [1, 1]], dtype=np.float32)
    assert top_k(vectors, np.array([.9, .9], dtype=np.float32), 2) == [2, 0]
    assert recall_at_k([2, 0], [2, 1]) == .5
