"""Generates a deterministic synthetic vector dataset for the ANN recall benchmark.

This is SYNTHETIC data, not a real embedding corpus (SIFT-1M or similar) -- PLAN.md names
that as the eventual dataset and this script does not pretend otherwise. It exists so the
Go index and Python's independent ground truth can be measured against each other without
a multi-gigabyte download blocking the benchmark from running at all. The vectors are drawn
from a mixture of Gaussian clusters rather than pure uniform noise, because clustered data
is closer to how real embeddings behave (semantically similar items form neighbourhoods)
and is a meaningfully harder, more realistic recall test than uniform random points, which
are usually close to equidistant from everything in high dimension.
"""

import argparse
import json

import numpy as np


def generate(num_vectors: int, dimension: int, num_queries: int, num_clusters: int, seed: int) -> dict:
    rng = np.random.default_rng(seed)
    centers = rng.normal(scale=3.0, size=(num_clusters, dimension)).astype(np.float32)
    assignments = rng.integers(0, num_clusters, size=num_vectors)
    vectors = centers[assignments] + rng.normal(scale=1.0, size=(num_vectors, dimension)).astype(np.float32)

    query_assignments = rng.integers(0, num_clusters, size=num_queries)
    queries = centers[query_assignments] + rng.normal(scale=1.0, size=(num_queries, dimension)).astype(np.float32)

    return {
        "dimension": dimension,
        "vectors": [{"id": f"v{i}", "values": vectors[i].tolist()} for i in range(num_vectors)],
        "queries": queries.tolist(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-vectors", type=int, default=5000)
    parser.add_argument("--dimension", type=int, default=128)
    parser.add_argument("--num-queries", type=int, default=200)
    parser.add_argument("--num-clusters", type=int, default=20)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    dataset = generate(args.num_vectors, args.dimension, args.num_queries, args.num_clusters, args.seed)
    with open(args.out, "w") as f:
        json.dump(dataset, f)
    print(f"wrote {args.num_vectors} vectors ({args.dimension}-d, {args.num_clusters} clusters) "
          f"and {args.num_queries} queries to {args.out}")


if __name__ == "__main__":
    main()
