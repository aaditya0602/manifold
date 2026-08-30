# Benchmark results: 20260830T172503Z

- Host: `AadityaPC`
- Kernel: `Linux AadityaPC 6.6.87.2-microsoft-standard-WSL2 #1 SMP PREEMPT_DYNAMIC Thu Jun  5 18:30:46 UTC 2025 x86_64 GNU/Linux`
- nproc: 8
- Core assignment: `{'k6': '0-1', 'lb': '2-3', 'backends': '4-7'}`
- Go: `go version go1.27.0 linux/amd64`
- nginx: `nginx version: nginx/1.28.3 (Ubuntu)`
- k6: `k6 v2.2.0 (commit/00a9a1b7f5, go1.26.5, linux/amd64)`
- Git commit: `8fc2fc9373409383b1054f00ebc39ce2285ea7ce` (clean)

> Drift check passed: first-cell RPS stable (4.6% drift observed between start and end of this run).

| Target | Strategy | Concurrency | Backend Latency | Runs | RPS min | RPS median | RPS max | p50 (ms) | p90 (ms) | p99 (ms) | p99.9 (ms) | Error Rate | LB CPU % | LB RSS (MB) | Δ RPS vs nginx | Notes |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| direct | direct | 50 | 1ms | 1 | 24764.5 | 24764.5 | 24764.5 | 1.60 | 3.12 | 7.18 | 11.05 | 0.00% | n/a | n/a | n/a |  |
| manifold | round_robin | 50 | 1ms | 1 | 13719.4 | 13719.4 | 13719.4 | 3.19 | 5.69 | 9.89 | 16.30 | 0.00% | 132.6% | 17.6 | -28.4% |  |
| nginx | round_robin | 50 | 1ms | 1 | 19173.7 | 19173.7 | 19173.7 | 1.97 | 4.27 | 11.00 | 17.93 | 0.00% | 82.6% | 18.9 | n/a |  |
