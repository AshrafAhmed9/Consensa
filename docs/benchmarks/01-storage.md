# Phase 1 storage benchmarks

Measured on 2026-09-01 with Go 1.26.4 on an Apple M5 (darwin/arm64), using:

`go test -run '^$' -bench 'Benchmark(SequentialWrite|PointRead)$' -benchtime=500ms -benchmem ./internal/storage`

| Benchmark | Result | Allocations |
| --- | ---: | ---: |
| Sequential write (`SyncEvery=1`) | 11.43 ms/op | 547 B/op, 10 allocs/op |
| Point read (64-key working set) | 998.7 ns/op | 4,910 B/op, 35 allocs/op |

These are baseline measurements, not a general throughput claim. The point-read benchmark
does not yet compare the Bloom path because the current table stays resident in memory;
that comparison belongs with the planned block-reader implementation.
