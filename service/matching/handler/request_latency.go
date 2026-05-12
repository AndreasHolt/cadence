package handler

import (
	"math"
	"sync"
	"time"
)

const requestLatencyEWMATau = 60 * time.Second

type requestLatencyTracker struct {
	mu            sync.Mutex
	latencyEWMAms float64
	lastUpdate    time.Time
	initialized   bool
}

func (t *requestLatencyTracker) Observe(duration time.Duration) float64 {
	return t.observeAt(time.Now(), duration)
}

func (t *requestLatencyTracker) observeAt(now time.Time, duration time.Duration) float64 {
	latencyMs := float64(duration) / float64(time.Millisecond)
	if latencyMs < 0 {
		latencyMs = 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.initialized {
		t.latencyEWMAms = latencyMs
		t.lastUpdate = now
		t.initialized = true
		return t.latencyEWMAms
	}

	dt := now.Sub(t.lastUpdate)
	if dt < 0 {
		dt = 0
	}
	alpha := 1 - math.Exp(-dt.Seconds()/requestLatencyEWMATau.Seconds())
	t.latencyEWMAms = alpha*latencyMs + (1-alpha)*t.latencyEWMAms
	t.lastUpdate = now
	return t.latencyEWMAms
}
