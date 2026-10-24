package transport

import (
	"time"
)

// Timer abstracts timer behaviors for deterministic testing.
type Timer interface {
	C() <-chan time.Time
	Reset(d time.Duration) bool
	Stop() bool
}

// Clock provides injectable time for Raft election and heartbeat scheduling.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
	Sleep(d time.Duration)
}

// RealClock implements Clock using the standard time package.
type RealClock struct{}

func NewRealClock() *RealClock {
	return &RealClock{}
}

func (c *RealClock) Now() time.Time {
	return time.Now()
}

func (c *RealClock) NewTimer(d time.Duration) Timer {
	return &realTimer{t: time.NewTimer(d)}
}

func (c *RealClock) Sleep(d time.Duration) {
	time.Sleep(d)
}

type realTimer struct {
	t *time.Timer
}

func (rt *realTimer) C() <-chan time.Time {
	return rt.t.C
}

func (rt *realTimer) Reset(d time.Duration) bool {
	return rt.t.Reset(d)
}

func (rt *realTimer) Stop() bool {
	return rt.t.Stop()
}
