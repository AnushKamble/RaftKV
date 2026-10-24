package simulator

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	raftkvv1 "raftkv/gen/raftkv/v1"
	"raftkv/internal/transport"
)

// RPCHandlers defines the receiver interface a simulated node provides.
type RPCHandlers interface {
	HandleRequestVote(ctx context.Context, args *raftkvv1.RequestVoteArgs) (*raftkvv1.RequestVoteReply, error)
	HandleAppendEntries(ctx context.Context, args *raftkvv1.AppendEntriesArgs) (*raftkvv1.AppendEntriesReply, error)
	HandleInstallSnapshot(ctx context.Context, args *raftkvv1.InstallSnapshotArgs) (*raftkvv1.InstallSnapshotReply, error)
}

// Packet encapsulates a queued network message between nodes.
type Packet struct {
	ID            uint64
	Source        string
	Target        string
	RPC           string
	Args          any
	ScheduledTime time.Duration
	ReplyChan     chan responseEnvelope
}

type responseEnvelope struct {
	reply any
	err   error
}

// NetworkSimulator provides deterministic message scheduling and fault injection.
type NetworkSimulator struct {
	mu             sync.Mutex
	seed           int64
	rnd            *rand.Rand
	clock          *VirtualClock
	trace          *TraceLogger
	handlers       map[string]RPCHandlers
	packets        []*Packet
	packetSeq      uint64
	partitions     map[string]int // nodeID -> partitionID
	dropRate       float64
	dupRate        float64
	minDelay       time.Duration
	maxDelay       time.Duration
	filterDropFunc func(src, dst, rpc string) bool
}

// NewNetworkSimulator initializes the deterministic network harness with a given seed.
func NewNetworkSimulator(seed int64, clock *VirtualClock, trace *TraceLogger) *NetworkSimulator {
	return &NetworkSimulator{
		seed:       seed,
		rnd:        rand.New(rand.NewSource(seed)),
		clock:      clock,
		trace:      trace,
		handlers:   make(map[string]RPCHandlers),
		partitions: make(map[string]int),
		minDelay:   10 * time.Millisecond,
		maxDelay:   50 * time.Millisecond,
	}
}

// RegisterNode binds a node's RPC handlers to the simulated network.
func (ns *NetworkSimulator) RegisterNode(nodeID string, handlers RPCHandlers) transport.Transport {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.handlers[nodeID] = handlers
	return &SimulatedNodeTransport{
		nodeID: nodeID,
		net:    ns,
	}
}

// SetFaults configures drop and duplication probabilities and delay boundaries.
func (ns *NetworkSimulator) SetFaults(dropRate, dupRate float64, minDelay, maxDelay time.Duration) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.dropRate = dropRate
	ns.dupRate = dupRate
	ns.minDelay = minDelay
	ns.maxDelay = maxDelay
}

// SetDropFilter sets a deterministic programmatic filter for dropping messages.
func (ns *NetworkSimulator) SetDropFilter(f func(src, dst, rpc string) bool) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.filterDropFunc = f
}

// Partition sets up network partitions. Nodes in different partition IDs cannot talk.
func (ns *NetworkSimulator) Partition(partitions map[string]int) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.partitions = make(map[string]int)
	for k, v := range partitions {
		ns.partitions[k] = v
	}
	if ns.trace != nil {
		ns.trace.Record(ns.clock.Elapsed(), "PARTITION", "network", "", fmt.Sprintf("%v", partitions))
	}
}

// Heal removes all network partitions.
func (ns *NetworkSimulator) Heal() {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.partitions = make(map[string]int)
	if ns.trace != nil {
		ns.trace.Record(ns.clock.Elapsed(), "HEAL", "network", "", "partitions cleared")
	}
}

// Send schedules an outbound RPC packet.
func (ns *NetworkSimulator) send(ctx context.Context, src, dst, rpc string, args any) (any, error) {
	ns.mu.Lock()

	// Check partition
	srcPart, hasSrc := ns.partitions[src]
	dstPart, hasDst := ns.partitions[dst]
	if hasSrc && hasDst && srcPart != dstPart {
		if ns.trace != nil {
			ns.trace.Record(ns.clock.Elapsed(), "DROP", src, dst, fmt.Sprintf("%s (partitioned)", rpc))
		}
		ns.mu.Unlock()
		return nil, fmt.Errorf("network partition: %s cannot reach %s", src, dst)
	}

	// Check programmatic filter
	if ns.filterDropFunc != nil && ns.filterDropFunc(src, dst, rpc) {
		if ns.trace != nil {
			ns.trace.Record(ns.clock.Elapsed(), "DROP", src, dst, fmt.Sprintf("%s (filter drop)", rpc))
		}
		ns.mu.Unlock()
		return nil, fmt.Errorf("network drop: message filtered out")
	}

	// Check probabilistic drop
	if ns.dropRate > 0 && ns.rnd.Float64() < ns.dropRate {
		if ns.trace != nil {
			ns.trace.Record(ns.clock.Elapsed(), "DROP", src, dst, fmt.Sprintf("%s (random loss)", rpc))
		}
		ns.mu.Unlock()
		return nil, fmt.Errorf("network drop: packet lost in transit")
	}

	// Calculate deterministic delay
	delay := ns.minDelay
	if ns.maxDelay > ns.minDelay {
		spread := int64(ns.maxDelay - ns.minDelay)
		delay += time.Duration(ns.rnd.Int63n(spread))
	}

	schedTime := ns.clock.Elapsed() + delay
	ns.packetSeq++
	pkt := &Packet{
		ID:            ns.packetSeq,
		Source:        src,
		Target:        dst,
		RPC:           rpc,
		Args:          args,
		ScheduledTime: schedTime,
		ReplyChan:     make(chan responseEnvelope, 1),
	}
	ns.packets = append(ns.packets, pkt)

	if ns.trace != nil {
		ns.trace.Record(ns.clock.Elapsed(), "SEND", src, dst, fmt.Sprintf("%s scheduled for +%s (pkt #%d)", rpc, delay, pkt.ID))
	}

	// Check duplication
	if ns.dupRate > 0 && ns.rnd.Float64() < ns.dupRate {
		ns.packetSeq++
		dupPkt := &Packet{
			ID:            ns.packetSeq,
			Source:        src,
			Target:        dst,
			RPC:           rpc,
			Args:          args,
			ScheduledTime: schedTime + 5*time.Millisecond,
			ReplyChan:     make(chan responseEnvelope, 1),
		}
		ns.packets = append(ns.packets, dupPkt)
		if ns.trace != nil {
			ns.trace.Record(ns.clock.Elapsed(), "DUPLICATE", src, dst, fmt.Sprintf("duplicated pkt #%d as #%d", pkt.ID, dupPkt.ID))
		}
	}

	ns.sortPacketsLocked()
	replyChan := pkt.ReplyChan
	ns.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-replyChan:
		return res.reply, res.err
	}
}

func (ns *NetworkSimulator) sortPacketsLocked() {
	sort.SliceStable(ns.packets, func(i, j int) bool {
		if ns.packets[i].ScheduledTime != ns.packets[j].ScheduledTime {
			return ns.packets[i].ScheduledTime < ns.packets[j].ScheduledTime
		}
		return ns.packets[i].ID < ns.packets[j].ID
	})
}

// Step delivers the next ready packet if scheduled time <= current virtual time.
// Returns true if a packet was delivered, false otherwise.
func (ns *NetworkSimulator) Step() bool {
	ns.mu.Lock()
	now := ns.clock.Elapsed()

	var candidateIdx = -1
	for i, p := range ns.packets {
		if p.ScheduledTime <= now {
			candidateIdx = i
			break
		}
	}

	if candidateIdx == -1 {
		ns.mu.Unlock()
		return false
	}

	pkt := ns.packets[candidateIdx]
	ns.packets = append(ns.packets[:candidateIdx], ns.packets[candidateIdx+1:]...)

	handler, ok := ns.handlers[pkt.Target]
	if ns.trace != nil {
		ns.trace.Record(now, "DELIVER", pkt.Source, pkt.Target, fmt.Sprintf("%s (pkt #%d)", pkt.RPC, pkt.ID))
	}
	ns.mu.Unlock()

	if !ok {
		pkt.ReplyChan <- responseEnvelope{err: fmt.Errorf("target node %s not found", pkt.Target)}
		return true
	}

	// Dispatch in background goroutine to simulate concurrent network arrival without blocking simulator loop
	go func(p *Packet, h RPCHandlers) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		var reply any
		var err error

		switch p.RPC {
		case "RequestVote":
			reply, err = h.HandleRequestVote(ctx, p.Args.(*raftkvv1.RequestVoteArgs))
		case "AppendEntries":
			reply, err = h.HandleAppendEntries(ctx, p.Args.(*raftkvv1.AppendEntriesArgs))
		case "InstallSnapshot":
			reply, err = h.HandleInstallSnapshot(ctx, p.Args.(*raftkvv1.InstallSnapshotArgs))
		default:
			err = fmt.Errorf("unknown RPC %s", p.RPC)
		}

		select {
		case p.ReplyChan <- responseEnvelope{reply: reply, err: err}:
		default:
		}
	}(pkt, handler)

	return true
}

// DeliverAllReady processes all packets that are scheduled up to current virtual time.
func (ns *NetworkSimulator) DeliverAllReady() int {
	count := 0
	for ns.Step() {
		count++
	}
	return count
}

// AdvanceAndDeliver moves virtual time forward by d and delivers all packets that become ready.
func (ns *NetworkSimulator) AdvanceAndDeliver(d time.Duration) int {
	ns.clock.Advance(d)
	return ns.DeliverAllReady()
}

// PendingCount returns the number of scheduled packets still waiting.
func (ns *NetworkSimulator) PendingCount() int {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	return len(ns.packets)
}

// SimulatedNodeTransport adapts NetworkSimulator to the transport.Transport interface for a specific node.
type SimulatedNodeTransport struct {
	nodeID string
	net    *NetworkSimulator
}

func (snt *SimulatedNodeTransport) SendRequestVote(ctx context.Context, target string, args *raftkvv1.RequestVoteArgs) (*raftkvv1.RequestVoteReply, error) {
	res, err := snt.net.send(ctx, snt.nodeID, target, "RequestVote", args)
	if err != nil {
		return nil, err
	}
	return res.(*raftkvv1.RequestVoteReply), nil
}

func (snt *SimulatedNodeTransport) SendAppendEntries(ctx context.Context, target string, args *raftkvv1.AppendEntriesArgs) (*raftkvv1.AppendEntriesReply, error) {
	res, err := snt.net.send(ctx, snt.nodeID, target, "AppendEntries", args)
	if err != nil {
		return nil, err
	}
	return res.(*raftkvv1.AppendEntriesReply), nil
}

func (snt *SimulatedNodeTransport) SendInstallSnapshot(ctx context.Context, target string, args *raftkvv1.InstallSnapshotArgs) (*raftkvv1.InstallSnapshotReply, error) {
	res, err := snt.net.send(ctx, snt.nodeID, target, "InstallSnapshot", args)
	if err != nil {
		return nil, err
	}
	return res.(*raftkvv1.InstallSnapshotReply), nil
}

func (snt *SimulatedNodeTransport) Close() error {
	return nil
}
