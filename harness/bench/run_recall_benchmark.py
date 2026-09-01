"""Runs the real Go ANN index (cmd/annbench) against a dataset and measures recall@k
against an independent Python brute-force ground truth (recall.py).

This is the connector docs/benchmarks/04-ann.md names as missing: a prior HNSW benchmark
in that file measured only raw search latency and explicitly said "this ... is not a
recall@10 measurement and must not be compared to real-corpus ANN benchmarks until the
pinned-corpus harness is connected to the Go index." This script is that connection.

The dataset is synthetic (see generate_dataset.py's docstring for why); the recall numbers
this script measures are real, non-fabricated numbers from actually running the Go binary
and actually computing ground truth independently in Python -- they are just not yet
numbers from a real embedding corpus, and this file does not claim otherwise.
"""

import argparse
import json
import subprocess
import sys
import time

import numpy as np

from recall import recall_at_k, top_k


def build_go_binary(repo_root: str) -> str:
    out_path = f"{repo_root}/harness/bench/.annbench"
    result = subprocess.run(
        ["go", "build", "-o", out_path, "./cmd/annbench"],
        cwd=repo_root, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError("building cmd/annbench failed")
    return out_path


def run_index(binary: str, dataset_path: str, index_kind: str, k: int, sweep: str, **extra_flags) -> dict:
    args = [binary, "-dataset", dataset_path, "-index", index_kind, "-k", str(k), "-sweep", sweep]
    for flag, value in extra_flags.items():
        args += [f"-{flag}", str(value)]
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"{binary} failed")
    return json.loads(result.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, help="path to a dataset JSON file from generate_dataset.py")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--hnsw-sweep", default="16,32,64,128")
    parser.add_argument("--ivfflat-sweep", default="1,2,4,8")
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

    binary = build_go_binary(args.repo_root)

    rows = []
    for index_kind, sweep_flag, param_name in [("hnsw", args.hnsw_sweep, "efSearch"), ("ivfflat", args.ivfflat_sweep, "nProbe")]:
        result = run_index(binary, args.dataset, index_kind, args.k, sweep_flag)
        for sweep in result["sweeps"]:
            recalls = []
            for r in sweep["results"]:
                expected_ids = {vector_ids[i] for i in ground_truth[r["query_index"]]}
                actual_ids = set(r["ids"])
                recalls.append(len(expected_ids & actual_ids) / len(expected_ids) if expected_ids else 1.0)
            mean_recall = sum(recalls) / len(recalls) if recalls else 0.0
            rows.append({
                "index": index_kind,
                param_name: sweep["param"],
                "mean_recall_at_k": round(mean_recall, 4),
                "mean_search_us": round(sweep["mean_search_us"], 1),
                "build_ms": round(result["build_ms"], 1),
            })

    print(f"\n{'index':<8} {'param':<10} {'recall@' + str(args.k):<12} {'mean search (us)':<18} build (ms)")
    for row in rows:
        param = row.get("efSearch", row.get("nProbe"))
        print(f"{row['index']:<8} {param:<10} {row['mean_recall_at_k']:<12} {row['mean_search_us']:<18} {row['build_ms']}")

    print(json.dumps(rows))


if __name__ == "__main__":
    main()
