# Phase 4 ANN baseline

Measured on 2026-09-01 with Go 1.26.4 on an Apple M5 (darwin/arm64):

`go test -run '^$' -bench BenchmarkHNSWSearch128 -benchtime=500ms -benchmem ./internal/ann`

| Corpus / query | `efSearch` | Result | Allocation |
| --- | ---: | ---: | ---: |
| 256 synthetic 128-dimension vectors / top-10 | 64 | 60,504 ns/op | 33,416 B/op, 632 allocs/op |

This measures the current scalar graph traversal only. It is not a recall@10 measurement
and must not be compared to real-corpus ANN benchmarks until the pinned-corpus harness is
connected to the Go index.
