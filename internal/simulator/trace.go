package simulator

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// TraceEvent records a single simulated occurrence (packet, timer, fault, transition).
type TraceEvent struct {
	Index     uint64        `json:"index"`
	SimTime   time.Duration `json:"sim_time_ms"`
	EventType string        `json:"event_type"`
	Source    string        `json:"source,omitempty"`
	Target    string        `json:"target,omitempty"`
	Details   string        `json:"details"`
}

// TraceLogger captures and outputs deterministic event history.
type TraceLogger struct {
	mu     sync.Mutex
	events []TraceEvent
	seq    uint64
}

func NewTraceLogger() *TraceLogger {
	return &TraceLogger{}
}

func (tl *TraceLogger) Record(simTime time.Duration, eventType, src, target, details string) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	tl.seq++
	ev := TraceEvent{
		Index:     tl.seq,
		SimTime:   simTime,
		EventType: eventType,
		Source:    src,
		Target:    target,
		Details:   details,
	}
	tl.events = append(tl.events, ev)
}

func (tl *TraceLogger) Events() []TraceEvent {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	res := make([]TraceEvent, len(tl.events))
	copy(res, tl.events)
	return res
}

func (tl *TraceLogger) String() string {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	var sb strings.Builder
	sb.WriteString("=== SIMULATION TRACE ===\n")
	for _, e := range tl.events {
		sb.WriteString(fmt.Sprintf("[%8s] #%04d %-12s %-8s -> %-8s | %s\n",
			e.SimTime, e.Index, e.EventType, e.Source, e.Target, e.Details))
	}
	sb.WriteString("========================\n")
	return sb.String()
}

func (tl *TraceLogger) ToJSON() ([]byte, error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return json.MarshalIndent(tl.events, "", "  ")
}
