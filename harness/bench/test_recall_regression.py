"""Fails if the Go HNSW index's recall@10 drops below a committed baseline.

A plain "recall >= 0.95" check would be flaky, since HNSW picks node levels at random.
This test fixes the seed, the dataset (size, dimension, clusters), the queries and the
HNSW settings, then compares against a baseline with a tolerance band. A drop below the
band fails. An unexplained rise only prints a warning, because it usually means the
ground-truth code broke rather than the index improving.

The settings are deliberately small and use efSearch=12, which is not the efSearch=64
behind the 98.6% figure in docs/benchmarks/04-ann.md. At efSearch=12 recall is below 1.0,
so a real regression shows up as a number change instead of "still 1.0".
"""

import json
import os
import subprocess
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from generate_dataset import generate  # noqa: E402
from recall import top_k  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Pinned exactly: changing any of these invalidates BASELINE_RECALL below and requires
# re-measuring it (see this file's own instructions in the assertion message).
NUM_VECTORS = 500
DIMENSION = 64
NUM_QUERIES = 50
NUM_CLUSTERS = 10
DATASET_SEED = 42
INDEX_SEED = 1
K = 10
EF_SEARCH = 12

BASELINE_RECALL = 0.974
TOLERANCE = 0.03


def _run_annbench(dataset_path: str, tmp_dir: str) -> dict:
    binary = os.path.join(tmp_dir, "annbench")
    build = subprocess.run(
        ["go", "build", "-o", binary, "./cmd/annbench"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    assert build.returncode == 0, f"building cmd/annbench failed:\n{build.stderr}"

    result = subprocess.run(
        [binary, "-dataset", dataset_path, "-k", str(K), "-sweep", str(EF_SEARCH), "-seed", str(INDEX_SEED)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, f"annbench failed:\n{result.stderr}"
    return json.loads(result.stdout)


def test_hnsw_recall_regression(tmp_path):
    dataset = generate(NUM_VECTORS, DIMENSION, NUM_QUERIES, NUM_CLUSTERS, DATASET_SEED)
    dataset_path = tmp_path / "dataset.json"
    dataset_path.write_text(json.dumps(dataset))

    out = _run_annbench(str(dataset_path), str(tmp_path))

    vectors = np.array([v["values"] for v in dataset["vectors"]], dtype=np.float32)
    vector_ids = [v["id"] for v in dataset["vectors"]]
    queries = np.array(dataset["queries"], dtype=np.float32)

    sweep = out["sweeps"][0]
    assert sweep["param"] == EF_SEARCH
    recalls = []
    for r in sweep["results"]:
        expected = {vector_ids[i] for i in top_k(vectors, queries[r["query_index"]], K)}
        actual = set(r["ids"])
        recalls.append(len(expected & actual) / len(expected) if expected else 1.0)
    mean_recall = sum(recalls) / len(recalls)

    floor = BASELINE_RECALL - TOLERANCE
    assert mean_recall >= floor, (
        f"recall@{K} regressed to {mean_recall:.4f}, below the floor {floor:.4f} "
        f"(baseline {BASELINE_RECALL} - tolerance {TOLERANCE}). This pinned config "
        "(seed, dataset shape, efSearch) should be deterministic -- investigate the HNSW "
        "insert/search path, do not just raise the baseline."
    )
    if mean_recall > BASELINE_RECALL + TOLERANCE:
        print(
            f"\nWARNING: recall@{K} improved to {mean_recall:.4f}, above "
            f"{BASELINE_RECALL + TOLERANCE:.4f}. Since this config is pinned and should be "
            "deterministic, an unexplained improvement usually means the ground truth "
            "computation changed, not that the index got better -- verify before updating "
            "BASELINE_RECALL in this file.",
            file=sys.stderr,
        )
