package crash

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"raftkv/internal/client"
	"raftkv/internal/config"
	raftkvv1 "raftkv/gen/raftkv/v1"
)

type NodeProcess struct {
	ID         string
	ConfigPath string
	DataDir    string
	Address    string
	cmd        *exec.Cmd
}

type SubprocessCluster struct {
	t          *testing.T
	baseDir    string
	binaryPath string
	Processes  map[string]*NodeProcess
	Addresses  []string
	NodeMap    map[string]string
}

func NewSubprocessCluster(t *testing.T, basePort int) *SubprocessCluster {
	baseDir, err := os.MkdirTemp("", "raftkv-subproc-*")
	if err != nil {
		t.Fatal(err)
	}

	// Locate or build raftkv-server binary
	binDir := filepath.Join(baseDir, "bin")
	_ = os.MkdirAll(binDir, 0755)
	binPath := filepath.Join(binDir, "raftkv-server.exe")

	buildCmd := exec.Command("go", "build", "-o", binPath, "../../cmd/raftkv-server")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build raftkv-server for crash test: %v\n%s", err, string(out))
	}

	nodeIDs := []string{"node-1", "node-2", "node-3"}
	addrs := make([]string, 3)
	dirs := make([]string, 3)
	cfgPaths := make([]string, 3)
	nodeMap := make(map[string]string)

	for i := 0; i < 3; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
		dirs[i] = filepath.Join(baseDir, nodeIDs[i])
		_ = os.MkdirAll(dirs[i], 0755)
		cfgPaths[i] = filepath.Join(baseDir, fmt.Sprintf("%s.yaml", nodeIDs[i]))
		nodeMap[nodeIDs[i]] = addrs[i]
	}

	// Generate config files
	for i := 0; i < 3; i++ {
		peers := make(map[string]string)
		for j := 0; j < 3; j++ {
			if i != j {
				peers[nodeIDs[j]] = addrs[j]
			}
		}

		rawCfg := config.NodeConfig{
			ID:                 nodeIDs[i],
			DataDir:            dirs[i],
			Address:            addrs[i],
			Peers:              peers,
			MinElectionTimeout: "150ms",
			MaxElectionTimeout: "300ms",
			HeartbeatInterval:  "50ms",
		}

		data := fmt.Sprintf(`id: "%s"
data_dir: "%s"
address: "%s"
min_election_timeout: "150ms"
max_election_timeout: "300ms"
heartbeat_interval: "50ms"
peers:
`, rawCfg.ID, filepath.ToSlash(rawCfg.DataDir), rawCfg.Address)

		for pID, pAddr := range peers {
			data += fmt.Sprintf("  %s: \"%s\"\n", pID, pAddr)
		}

		if err := os.WriteFile(cfgPaths[i], []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}

	sc := &SubprocessCluster{
		t:          t,
		baseDir:    baseDir,
		binaryPath: binPath,
		Processes:  make(map[string]*NodeProcess),
		Addresses:  addrs,
		NodeMap:    nodeMap,
	}

	for i := 0; i < 3; i++ {
		sc.Processes[nodeIDs[i]] = &NodeProcess{
			ID:         nodeIDs[i],
			ConfigPath: cfgPaths[i],
			DataDir:    dirs[i],
			Address:    addrs[i],
		}
	}

	return sc
}

func (sc *SubprocessCluster) StartNode(id string) error {
	p, ok := sc.Processes[id]
	if !ok {
		return fmt.Errorf("unknown node ID %s", id)
	}

	cmd := exec.Command(sc.binaryPath, "--config", p.ConfigPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start node %s: %w", id, err)
	}
	p.cmd = cmd
	return nil
}

func (sc *SubprocessCluster) StartAll() error {
	for id := range sc.Processes {
		if err := sc.StartNode(id); err != nil {
			return err
		}
	}
	return nil
}

func (sc *SubprocessCluster) KillNode(id string) error {
	p, ok := sc.Processes[id]
	if !ok || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	err := p.cmd.Process.Kill()
	_ = p.cmd.Wait()
	p.cmd = nil
	return err
}

func (sc *SubprocessCluster) StopAll() {
	for id := range sc.Processes {
		_ = sc.KillNode(id)
	}
	_ = os.RemoveAll(sc.baseDir)
}

func (sc *SubprocessCluster) WaitForLeader(timeout time.Duration) (string, string) {
	cli := client.NewClient(sc.Addresses)
	defer cli.Close()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, p := range sc.Processes {
			if p.cmd == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			st, err := cli.GetStatus(ctx, p.Address)
			cancel()
			if err == nil && st.Role == "Leader" {
				return id, p.Address
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	sc.t.Fatal("timed out waiting for leader in subprocess cluster")
	return "", ""
}

func (sc *SubprocessCluster) QueryStatus(addr string) (*raftkvv1.GetStatusReply, error) {
	cli := client.NewClient([]string{addr})
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	return cli.GetStatus(ctx, addr)
}
