package linearizability

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raftkv/internal/client"
	"raftkv/internal/config"
	"raftkv/internal/linearizability"
	"raftkv/internal/protocol"
	"raftkv/internal/runtime"
)

type clusterFixture struct {
	nodes   []*runtime.Node
	dirs    []string
	addrs   []string
	nodeMap map[string]string
	baseDir string
}

func startFixture(t *testing.T, basePort int) *clusterFixture {
	baseDir, err := os.MkdirTemp("", "raftkv-lin-cluster-*")
	if err != nil {
		t.Fatal(err)
	}

	nodeIDs := []string{"node-1", "node-2", "node-3"}
	addrs := make([]string, 3)
	dirs := make([]string, 3)
	nodeMap := make(map[string]string)

	for i := 0; i < 3; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
		dirs[i] = filepath.Join(baseDir, nodeIDs[i])
		nodeMap[nodeIDs[i]] = addrs[i]
	}

	var nodes []*runtime.Node
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

		n, err := runtime.NewNode(cfg)
		if err != nil {
			t.Fatalf("failed to create node %s: %v", nodeIDs[i], err)
		}
		if err := n.Start(); err != nil {
			t.Fatalf("failed to start node %s: %v", nodeIDs[i], err)
		}
		nodes = append(nodes, n)
	}

	return &clusterFixture{
		nodes:   nodes,
		dirs:    dirs,
		addrs:   addrs,
		nodeMap: nodeMap,
		baseDir: baseDir,
	}
}

func (c *clusterFixture) close() {
	for _, n := range c.nodes {
		n.Stop()
	}
	_ = os.RemoveAll(c.baseDir)
}

func TestClusterLinearizability_ConcurrentWorkload(t *testing.T) {
	cluster := startFixture(t, 54051)
	defer cluster.close()

	// Wait for leader election
	time.Sleep(1 * time.Second)

	var historyMu sync.Mutex
	var history []linearizability.OperationRecord
	var opSeq uint64

	workers := 3
	opsPerWorker := 10
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			cli := client.NewClient(cluster.addrs)
			cli.SetNodeAddressMap(cluster.nodeMap)
			defer cli.Close()

			keys := []string{"acc1", "acc2"}

			for i := 0; i < opsPerWorker; i++ {
				opID := atomic.AddUint64(&opSeq, 1)
				key := keys[rand.Intn(len(keys))]
				opType := rand.Intn(3) // 0: Put, 1: Get, 2: CAS

				rec := linearizability.OperationRecord{
					ID:         opID,
					ClientID:   fmt.Sprintf("worker-%d", workerID),
					Key:        key,
					InvokeTime: time.Now().UnixNano(),
				}

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

				switch opType {
				case 0: // Put
					rec.Op = protocol.OpPut
					rec.Value = fmt.Sprintf("v-%d-%d", workerID, i)
					err := cli.Put(ctx, key, rec.Value)
					rec.ReturnTime = time.Now().UnixNano()
					if err != nil {
						rec.TimedOut = true
					}

				case 1: // Get
					rec.Op = protocol.OpGet
					val, found, err := cli.Get(ctx, key)
					rec.ReturnTime = time.Now().UnixNano()
					if err != nil {
						rec.TimedOut = true
					} else {
						rec.ObservedValue = val
						rec.ObservedFound = found
					}

				case 2: // CAS
					rec.Op = protocol.OpCAS
					rec.Expected = "" // try initializing or swapping
					rec.Value = fmt.Sprintf("cas-%d-%d", workerID, i)
					swapped, curVal, err := cli.CompareAndSwap(ctx, key, rec.Expected, rec.Value)
					rec.ReturnTime = time.Now().UnixNano()
					if err != nil {
						rec.TimedOut = true
					} else {
						rec.ObservedSwapped = swapped
						rec.ObservedValue = curVal
					}
				}
				cancel()

				historyMu.Lock()
				history = append(history, rec)
				historyMu.Unlock()

				time.Sleep(20 * time.Millisecond)
			}
		}(w)
	}

	wg.Wait()

	t.Logf("Collected %d operations from real gRPC cluster execution.", len(history))

	// Save captured history to results/cluster_history.json
	_ = os.MkdirAll("../../results", 0755)
	historyPath := filepath.Join("../../results", "cluster_history.json")
	data, _ := json.MarshalIndent(history, "", "  ")
	_ = os.WriteFile(historyPath, data, 0644)

	// Run bounded linearizability checker
	checker := linearizability.NewChecker(100000)
	result := checker.Check(history)

	if result.Outcome != linearizability.OutcomePass {
		t.Fatalf("Linearizability check failed with %s (steps: %d):\n%s",
			result.Outcome, result.StepsExplored, result.Counterexample)
	}

	t.Logf("LINEARIZABILITY PASSED: Witness found (%d steps explored, %d operations linearized).",
		result.StepsExplored, len(result.Witness))
}
