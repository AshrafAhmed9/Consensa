# HNSW recall and latency

Measured 2026-09-01, Go 1.26.4, Apple M5 (darwin/arm64).

`cmd/annbench` builds the real `internal/ann` HNSW index and prints its search results as
JSON. `harness/bench/run_recall_benchmark.py` computes an independent brute-force ground
truth in NumPy (`harness/bench/recall.py`) and measures recall@10 against it, so nothing
is trusted from inside the Go binary.

The dataset is synthetic, not a real embedding corpus: 5,000 vectors, 128 dimensions,
drawn from 20 Gaussian clusters (`harness/bench/generate_dataset.py`, seed 1), plus 200
queries from the same clusters. Clustered data is a harder recall test than uniform random
points, which are all roughly equidistant in high dimension.

```
python3 harness/bench/generate_dataset.py --num-vectors 5000 --dimension 128 \
    --num-queries 200 --num-clusters 20 --seed 1 --out /tmp/ann_dataset.json
python3 harness/bench/run_recall_benchmark.py --dataset /tmp/ann_dataset.json --repo-root .
```

| efSearch | recall@10 | Mean search | Build time |
| ---: | ---: | ---: | ---: |
| 16 | 0.896 | 52.1 µs | 2,515.8 ms |
| 32 | 0.966 | 92.3 µs | 2,515.8 ms |
| 64 | 0.986 | 156.3 µs | 2,515.8 ms |
| 128 | 1.000 | 302.4 µs | 2,515.8 ms |

These numbers are from synthetic data. Running the same harness on a real embedding set
(SIFT-1M or similar) has not been done.
