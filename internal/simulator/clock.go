package simulator

import (
	"sort"
	"sync"
	"time"

	"raftkv/internal/transport"
)

// VirtualClock provides deterministic manual time advancement.
type VirtualClock struct {
	mu      sync.Mutex
	base    time.Time
	elapsed time.Duration
	timers  []*virtualTimer
	trace   *TraceLogger
}

// NewVirtualClock creates a virtual clock with optional trace logger.
func NewVirtualClock(trace *TraceLogger) *VirtualClock {
	return &VirtualClock{
		base:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		trace:  trace,
		timers: make([]*virtualTimer, 0),
	}
}

func (vc *VirtualClock) Now() time.Time {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	return vc.base.Add(vc.elapsed)
}

func (vc *VirtualClock) Elapsed() time.Duration {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	return vc.elapsed
}

func (vc *VirtualClock) NewTimer(d time.Duration) transport.Timer {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	vt := &virtualTimer{
		clock:  vc,
		expiry: vc.elapsed + d,
		ch:     make(chan time.Time, 1),
		active: true,
	}
	vc.timers = append(vc.timers, vt)
	return vt
}

func (vc *VirtualClock) Sleep(d time.Duration) {
	vc.Advance(d)
}

// Advance moves virtual time forward by d, firing any timers that expire.
func (vc *VirtualClock) Advance(d time.Duration) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	target := vc.elapsed + d
	vc.elapsed = target

	// Sort timers by expiry to ensure deterministic fire order
	sort.SliceStable(vc.timers, func(i, j int) bool {
		return vc.timers[i].expiry < vc.timers[j].expiry
	})

	var remaining []*virtualTimer
	for _, vt := range vc.timers {
		if vt.active && vt.expiry <= target {
			vt.active = false
			select {
			case vt.ch <- vc.base.Add(vt.expiry):
			default:
			}
			if vc.trace != nil {
				vc.trace.Record(vt.expiry, "TIMER_FIRE", "clock", "", "timer expired")
			}
		} else if vt.active {
			remaining = append(remaining, vt)
		}
	}
	vc.timers = remaining
}

type virtualTimer struct {
	clock  *VirtualClock
	expiry time.Duration
	ch     chan time.Time
	active bool
}

func (vt *virtualTimer) C() <-chan time.Time {
	return vt.ch
}

func (vt *virtualTimer) Reset(d time.Duration) bool {
	vt.clock.mu.Lock()
	defer vt.clock.mu.Unlock()

	wasActive := vt.active
	vt.active = true
	vt.expiry = vt.clock.elapsed + d

	// Drain any unconsumed tick
	select {
	case <-vt.ch:
	default:
	}

	// Re-add to timers list if not already present
	found := false
	for _, t := range vt.clock.timers {
		if t == vt {
			found = true
			break
		}
	}
	if !found {
		vt.clock.timers = append(vt.clock.timers, vt)
	}

	return wasActive
}

func (vt *virtualTimer) Stop() bool {
	vt.clock.mu.Lock()
	defer vt.clock.mu.Unlock()

	wasActive := vt.active
	vt.active = false
	return wasActive
}
