# RaftKV — Experimental Benchmark Evaluation

This document presents experimental performance measurements of RaftKV evaluated on a Windows local loopback environment.

## Execution Environment
- **OS**: Windows 11 AMD64 (`windows/amd64`)
- **Go Version**: `go1.27.0`
- **CPUs**: 32 logical cores
- **Storage**: Solid-State Disk (NVMe), synchronous `Sync()` on every committed WAL record frame
- **Cluster Size**: 3 nodes (single-shard Raft group)
- **Transport**: Real gRPC over TCP loopback (`127.0.0.1:55051-55053`)

---

## Measured Performance Results

| Workload Configuration | Throughput (ops/sec) | p50 Latency (ms) | p95 Latency (ms) | p99 Latency (ms) | Durability Policy |
|---|---|---|---|---|---|
| **Single-Node In-Memory Baseline** (10,000 ops) | **630,147.5** | < 0.01 ms | < 0.01 ms | < 0.01 ms | In-memory only (no WAL) |
| **3-Node RaftKV Cluster** (200 ops, 4 concurrent workers) | **766.0** | **5.15 ms** | **6.31 ms** | **7.40 ms** | Synchronous Fsync on every WAL record |

---

## Analysis & Bottleneck Identification

1. **In-Memory vs. Replicated Storage**:
   - The in-memory baseline processes over 630k ops/sec. In contrast, 3-node RaftKV achieves ~766 ops/sec with a p50 latency of 5.15ms.
2. **Dominant Bottleneck: Disk Fsync**:
   - In RaftKV, safety invariants mandate that the leader and followers execute synchronous `Sync()` on the WAL file descriptor before acknowledging AppendEntries or client responses.
   - On standard NVMe solid-state drives, individual disk fsync flushes take between 1.5ms and 3.5ms per flush. With round-trip network RPC latency, a 5.15ms p50 latency aligns directly with theoretical storage bounds.
3. **Reproducibility**:
   - Benchmark data is persisted in `results/benchmark_results.json` and `results/benchmark_results.csv`.
   - To rerun:
     ```bash
     go run ./cmd/raftkv-bench -ops=200 -concurrency=4
     ```
