# Phase 4 ANN baseline

Measured on 2026-09-01 with Go 1.26.4 on an Apple M5 (darwin/arm64):

`go test -run '^$' -bench BenchmarkHNSWSearch128 -benchtime=500ms -benchmem ./internal/ann`

| Corpus / query | `efSearch` | Result | Allocation |
| --- | ---: | ---: | ---: |
| 256 synthetic 128-dimension vectors / top-10 | 64 | 60,504 ns/op | 33,416 B/op, 632 allocs/op |

This measures the current scalar graph traversal only; it is not a recall measurement.

## Recall@10, HNSW vs IVFFlat

The pinned-corpus harness this file used to say was missing now exists:
`cmd/annbench` runs the real `internal/ann` index and prints its results as JSON;
`harness/bench/run_recall_benchmark.py` computes an independent brute-force ground truth
in NumPy (`harness/bench/recall.py`) and measures recall@10 against it. Both index kinds
run against the *same* dataset in the *same* process invocation, so the comparison is
apples-to-apples.

**The dataset is synthetic, not a real embedding corpus (SIFT-1M or similar) — stated
plainly per the honesty rule.** 5,000 vectors, 128 dimensions, drawn from 20 Gaussian
clusters (`harness/bench/generate_dataset.py`, seed 1) plus 200 queries from the same
clusters — clustered rather than uniform-random specifically because clustered structure
is closer to how real embeddings behave and is a harder, more meaningful recall test than
points that are all roughly equidistant in high dimension.

Measured 2026-09-01, Go 1.26.4, Apple M5 (darwin/arm64):

```
python3 harness/bench/generate_dataset.py --num-vectors 5000 --dimension 128 \
    --num-queries 200 --num-clusters 20 --seed 1 --out /tmp/ann_dataset.json
python3 harness/bench/run_recall_benchmark.py --dataset /tmp/ann_dataset.json --repo-root .
```

| Index | Param | recall@10 | Mean search | Build time |
| --- | ---: | ---: | ---: | ---: |
| HNSW | efSearch=16 | 0.896 | 52.1 µs | 2,515.8 ms |
| HNSW | efSearch=32 | 0.966 | 92.3 µs | 2,515.8 ms |
| HNSW | efSearch=64 | 0.986 | 156.3 µs | 2,515.8 ms |
| HNSW | efSearch=128 | 1.000 | 302.4 µs | 2,515.8 ms |
| IVFFlat | nProbe=1 | 0.834 | 132.6 µs | 15.3 ms |
| IVFFlat | nProbe=2 | 0.970 | 223.6 µs | 15.3 ms |
| IVFFlat | nProbe=4 | 0.989 | 421.6 µs | 15.3 ms |
| IVFFlat | nProbe=8 | 1.000 | 630.1 µs | 15.3 ms |

**Reading this honestly:** HNSW clears the plan's recall@10 ≥ 0.95 target at efSearch=32
already and is comfortably past it by efSearch=64, at a build cost roughly 165× IVFFlat's
(IVFFlat's "build" here is just bucketing vectors by nearest of 16 pre-chosen centroids —
see the caveat below — not real training cost). At matched recall (~0.99), HNSW's
efSearch=64 search (156 µs) beats IVFFlat's nProbe=4 search (422 µs) by roughly 2.7×. This
is the actual tradeoff HNSW's extra build-time investment buys: cheaper reads at query
time, which is the right thing to pay for in a system that is written far less often than
it is searched.

**IVFFlat's centroids are not k-means-trained** — `cmd/annbench` seeds them as the first
16 vectors in the dataset (`-centroids`, default 16), which `internal/ann/ivfflat.go`'s own
doc comment calls out as a deliberately simple baseline. A properly trained IVFFlat would
likely need a lower nProbe for the same recall; this table is honest about not having built
that yet, and the comparison above should be read as "a simple baseline vs. HNSW," not as
IVFFlat's ceiling.

**Determinism:** `harness/bench/test_recall_regression.py` pins the RNG seed, dataset
shape, and HNSW parameters, and asserts recall@10 stays within a committed tolerance band
of a baseline measured the same way — see that file's docstring for why a bare threshold
assertion would be a flake generator, and PLAN.md Phase 4's note on the same point.

**Still missing:** a real embedding corpus. The next step this table is honest about not
yet taking is running the identical harness against SIFT-1M or a real sentence-embedding
dump instead of the synthetic clusters above.
