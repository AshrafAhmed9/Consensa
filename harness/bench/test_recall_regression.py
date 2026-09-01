"""Deterministic recall@k regression gate for the real Go HNSW index.

PLAN.md is explicit about why a bare `recall >= threshold` assertion is a flake generator:
HNSW's level assignment is randomized, so a fresh build's recall varies run to run unless
everything about the run is pinned. This test pins the RNG seed, the dataset (size,
dimension, cluster count), the query set, and the HNSW parameters, and compares against a
COMMITTED baseline with a tolerance band -- fail on a drop below the band, only warn (not
fail) on an unexplained improvement, since that usually means the ground truth broke
rather than that the index got better.

This config is deliberately small and NOT the number quoted in docs/benchmarks/04-ann.md
as the recall@10 claim -- efSearch=12 here is chosen specifically because it sits below
saturation (recall < 1.0) so an actual regression has room to show up as a number change,
not just "still 1.0". The larger-scale, higher-efSearch numbers in the docs are the
production-relevant claim; this is the tripwire.
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
        [binary, "-dataset", dataset_path, "-index", "hnsw", "-k", str(K), "-sweep", str(EF_SEARCH), "-seed", str(INDEX_SEED)],
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
