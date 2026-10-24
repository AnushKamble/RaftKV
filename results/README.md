# RaftKV — Experimental Benchmark Results

This directory contains raw experimental measurements, benchmark artifacts, and reproduction scripts for RaftKV.

## Directory Structure
- `raw/`: Raw CSV and JSON benchmark runs, tagged by timestamp and commit SHA.
- `plots/`: Generated SVG/PNG charts visualizing throughput, latency distribution (p50, p95, p99), and recovery convergence.
- `README.md`: Measured summary tables, execution environment metadata, and reproduction commands.

## Reproducing Benchmarks
To regenerate benchmarks on your local machine:
```bash
# Via PowerShell (Windows)
powershell -ExecutionPolicy Bypass -File ./scripts/dev.ps1 bench

# Via Makefile (Linux/macOS)
make bench
```

All benchmark commands record CPU model, OS version, Go runtime version, disk storage type, fsync policy, concurrency level, and payload size.
