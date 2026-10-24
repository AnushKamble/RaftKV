package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"raftkv/internal/client"
	"raftkv/internal/config"
	"raftkv/internal/protocol"
	internalRuntime "raftkv/internal/runtime"
	"raftkv/internal/statemachine"
)

type BenchmarkMetric struct {
	Name            string        `json:"benchmark_name"`
	TotalOps        int           `json:"total_ops"`
	DurationMs      float64       `json:"duration_ms"`
	ThroughputOpsSec float64      `json:"throughput_ops_sec"`
	P50LatencyMs    float64       `json:"p50_latency_ms"`
	P95LatencyMs    float64       `json:"p95_latency_ms"`
	P99LatencyMs    float64       `json:"p99_latency_ms"`
	MinLatencyMs    float64       `json:"min_latency_ms"`
	MaxLatencyMs    float64       `json:"max_latency_ms"`
	DurabilityMode  string        `json:"durability_mode"`
	Transport       string        `json:"transport"`
}

type BenchmarkReport struct {
	Timestamp      string            `json:"timestamp"`
	GoVersion      string            `json:"go_version"`
	OS             string            `json:"os"`
	Architecture   string            `json:"architecture"`
	NumCPU         int               `json:"num_cpu"`
	Results        []BenchmarkMetric `json:"results"`
}

func main() {
	opsFlag := flag.Int("ops", 200, "Number of operations to benchmark in cluster mode")
	concurrencyFlag := flag.Int("concurrency", 4, "Number of concurrent client workers")
	outDirFlag := flag.String("out", "results", "Output directory for benchmark results")
	flag.Parse()

	report := BenchmarkReport{
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		GoVersion:    runtime.Version(),
		OS:           runtime.GOOS,
		Architecture: runtime.GOARCH,
		NumCPU:       runtime.NumCPU(),
	}

	fmt.Println("======================================================================")
	fmt.Printf("RaftKV Benchmark Harness\n")
	fmt.Printf("Platform: %s/%s | Go: %s | CPUs: %d\n", runtime.GOOS, runtime.GOARCH, runtime.Version(), runtime.NumCPU())
	fmt.Println("======================================================================")

	// 1. Benchmark Single-Node In-Memory Baseline
	fmt.Println("\nRunning: Single-Node In-Memory Baseline (10,000 ops)...")
	memMetric := runInMemoryBenchmark(10000)
	report.Results = append(report.Results, memMetric)

	// 2. Benchmark Three-Node Real gRPC Cluster with Durable WAL Fsync
	fmt.Printf("\nRunning: Three-Node Real gRPC RaftKV Cluster (%d ops, %d workers, Fsync=true)...\n", *opsFlag, *concurrencyFlag)
	clusterMetric := runClusterBenchmark(*opsFlag, *concurrencyFlag)
	report.Results = append(report.Results, clusterMetric)

	// Output summary table
	fmt.Println("\n========================== BENCHMARK RESULTS ==========================")
	fmt.Printf("%-35s %-12s %-10s %-10s %-10s\n", "Benchmark Target", "Throughput", "p50 (ms)", "p95 (ms)", "p99 (ms)")
	fmt.Println("----------------------------------------------------------------------")
	for _, m := range report.Results {
		fmt.Printf("%-35s %8.1f op/s %10.2f %10.2f %10.2f\n",
			m.Name, m.ThroughputOpsSec, m.P50LatencyMs, m.P95LatencyMs, m.P99LatencyMs)
	}
	fmt.Println("======================================================================")

	// Persist JSON and CSV artifacts
	_ = os.MkdirAll(*outDirFlag, 0755)

	jsonPath := filepath.Join(*outDirFlag, "benchmark_results.json")
	data, _ := json.MarshalIndent(report, "", "  ")
	_ = os.WriteFile(jsonPath, data, 0644)
	fmt.Printf("Saved raw JSON report to: %s\n", jsonPath)

	csvPath := filepath.Join(*outDirFlag, "benchmark_results.csv")
	writeCSV(csvPath, report.Results)
	fmt.Printf("Saved raw CSV data to: %s\n", csvPath)
}

func runInMemoryBenchmark(ops int) BenchmarkMetric {
	sm := statemachine.NewKVStateMachine()
	latencies := make([]float64, ops)

	start := time.Now()
	for i := 0; i < ops; i++ {
		opStart := time.Now()
		sm.Apply(protocol.LogEntry{
			Index: protocol.Index(i + 1),
			Term:  1,
			Command: protocol.Command{
				Op:    protocol.OpPut,
				Key:   fmt.Sprintf("mem-k-%d", i),
				Value: "mem-v",
			},
		})
		latencies[i] = float64(time.Since(opStart).Microseconds()) / 1000.0
	}
	totalDuration := time.Since(start)

	return computeMetrics("Single-Node In-Memory Baseline", ops, totalDuration, latencies, "None (in-memory)", "Local memory")
}

func runClusterBenchmark(totalOps, concurrency int) BenchmarkMetric {
	baseDir, err := os.MkdirTemp("", "raftkv-bench-cluster-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(baseDir)

	basePort := 55051
	nodeIDs := []string{"bench-node-1", "bench-node-2", "bench-node-3"}
	addrs := make([]string, 3)
	dirs := make([]string, 3)
	nodeMap := make(map[string]string)

	for i := 0; i < 3; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
		dirs[i] = filepath.Join(baseDir, nodeIDs[i])
		nodeMap[nodeIDs[i]] = addrs[i]
	}

	var nodes []*internalRuntime.Node
	for i := 0; i < 3; i++ {
		peers := make(map[string]string)
		for j := 0; j < 3; j++ {
			if i != j {
				peers[nodeIDs[j]] = addrs[j]
			}
		}

		cfg := &config.ParsedNodeConfig{
			ID:                 nodeIDs[i],
			DataDir:            dirs[i],
			Address:            addrs[i],
			Peers:              peers,
			MinElectionTimeout: 150 * time.Millisecond,
			MaxElectionTimeout: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
		}

		n, err := internalRuntime.NewNode(cfg)
		if err != nil {
			panic(err)
		}
		if err := n.Start(); err != nil {
			panic(err)
		}
		nodes = append(nodes, n)
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	// Wait for leader
	time.Sleep(1 * time.Second)

	// Warm-up phase (30 ops)
	warmupCli := client.NewClient(addrs)
	warmupCli.SetNodeAddressMap(nodeMap)
	for i := 0; i < 30; i++ {
		_ = warmupCli.Put(context.Background(), fmt.Sprintf("warmup-%d", i), "wval")
	}
	warmupCli.Close()

	// Measured workload
	opsPerWorker := totalOps / concurrency
	var latenciesMu sync.Mutex
	var allLatencies []float64

	var wg sync.WaitGroup
	benchStart := time.Now()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			cli := client.NewClient(addrs)
			cli.SetNodeAddressMap(nodeMap)
			defer cli.Close()

			for i := 0; i < opsPerWorker; i++ {
				opStart := time.Now()
				key := fmt.Sprintf("key-w%d-%d", workerID, i)
				val := fmt.Sprintf("val-%d", i)

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := cli.Put(ctx, key, val)
				cancel()

				if err == nil {
					durMs := float64(time.Since(opStart).Microseconds()) / 1000.0
					latenciesMu.Lock()
					allLatencies = append(allLatencies, durMs)
					latenciesMu.Unlock()
				}
			}
		}(w)
	}

	wg.Wait()
	totalDuration := time.Since(benchStart)

	return computeMetrics("3-Node RaftKV Cluster (gRPC+WAL)", len(allLatencies), totalDuration, allLatencies, "Fsync-per-record", "TCP loopback (gRPC)")
}

func computeMetrics(name string, totalOps int, duration time.Duration, latencies []float64, durability, transport string) BenchmarkMetric {
	if len(latencies) == 0 {
		return BenchmarkMetric{Name: name}
	}

	sort.Float64s(latencies)
	p50 := latencies[int(float64(len(latencies))*0.50)]
	p95 := latencies[int(float64(len(latencies))*0.95)]
	p99 := latencies[int(float64(len(latencies))*0.99)]

	throughput := float64(totalOps) / duration.Seconds()

	return BenchmarkMetric{
		Name:            name,
		TotalOps:        totalOps,
		DurationMs:      float64(duration.Milliseconds()),
		ThroughputOpsSec: throughput,
		P50LatencyMs:    p50,
		P95LatencyMs:    p95,
		P99LatencyMs:    p99,
		MinLatencyMs:    latencies[0],
		MaxLatencyMs:    latencies[len(latencies)-1],
		DurabilityMode:  durability,
		Transport:       transport,
	}
}

func writeCSV(path string, results []BenchmarkMetric) {
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	_ = w.Write([]string{"Target", "Ops", "DurationMs", "Throughput_OpsSec", "p50_ms", "p95_ms", "p99_ms", "Durability", "Transport"})
	for _, r := range results {
		_ = w.Write([]string{
			r.Name,
			fmt.Sprintf("%d", r.TotalOps),
			fmt.Sprintf("%.2f", r.DurationMs),
			fmt.Sprintf("%.2f", r.ThroughputOpsSec),
			fmt.Sprintf("%.2f", r.P50LatencyMs),
			fmt.Sprintf("%.2f", r.P95LatencyMs),
			fmt.Sprintf("%.2f", r.P99LatencyMs),
			r.DurabilityMode,
			r.Transport,
		})
	}
}
