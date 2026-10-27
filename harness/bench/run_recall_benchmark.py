"""Measures how good the Go HNSW index (cmd/annbench) is at finding true nearest neighbours.

It does three things:
  1. Computes the exact nearest neighbours for every query by brute force (recall.py).
  2. Runs cmd/annbench, which builds the HNSW index and answers the same queries.
  3. Reports recall@k (what fraction of the true top k the index returned) and the mean
     search time, for each efSearch value.

The dataset is synthetic (see generate_dataset.py), so these numbers say nothing about
real embedding data. They are real measurements of the Go index, though: the ground
truth is computed here in Python, not by the code being tested.
"""

import argparse
import json
import subprocess
import sys
import time

import numpy as np

from recall import recall_at_k, top_k


def build_go_binary(repo_root: str) -> str:
    """Builds cmd/annbench and returns the path to the binary."""
    out_path = f"{repo_root}/harness/bench/.annbench"
    result = subprocess.run(["go", "build", "-o", out_path, "./cmd/annbench"], cwd=repo_root, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError("building cmd/annbench failed")
    return out_path


def run_annbench(binary: str, dataset_path: str, k: int, sweep: str) -> dict:
    """Runs cmd/annbench and returns its JSON output."""
    result = subprocess.run([binary, "-dataset", dataset_path, "-k", str(k), "-sweep", sweep], capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError("cmd/annbench failed")
    return json.loads(result.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, help="path to a dataset JSON file from generate_dataset.py")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--sweep", default="16,32,64,128", help="comma-separated efSearch values")
    args = parser.parse_args()

    with open(args.dataset) as f:
        dataset = json.load(f)
    vector_ids = [v["id"] for v in dataset["vectors"]]
    vectors = np.array([v["values"] for v in dataset["vectors"]], dtype=np.float32)
    queries = np.array(dataset["queries"], dtype=np.float32)

    print(f"computing brute-force ground truth for {len(queries)} queries over {len(vectors)} vectors...")
    start = time.time()
    ground_truth = [top_k(vectors, q, args.k) for q in queries]
    print(f"ground truth computed in {time.time() - start:.2f}s")

    result = run_annbench(build_go_binary(args.repo_root), args.dataset, args.k, args.sweep)

    rows = []
    for sweep in result["sweeps"]:
        recalls = []
        for r in sweep["results"]:
            # Compare IDs, not positions: annbench reports vector IDs.
            expected = [vector_ids[i] for i in ground_truth[r["query_index"]]]
            recalls.append(recall_at_k(expected, r["ids"]))
        rows.append({
            "efSearch": sweep["param"],
            "mean_recall_at_k": round(sum(recalls) / len(recalls), 4),
            "mean_search_us": round(sweep["mean_search_us"], 1),
            "build_ms": round(result["build_ms"], 1),
        })

    print(f"\n{'efSearch':<10} {'recall@' + str(args.k):<12} {'mean search (us)':<18} build (ms)")
    for row in rows:
        print(f"{row['efSearch']:<10} {row['mean_recall_at_k']:<12} {row['mean_search_us']:<18} {row['build_ms']}")
    print(json.dumps(rows))


if __name__ == "__main__":
    main()
