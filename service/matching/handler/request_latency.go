package handler

import (
	"sync"
	"time"
)

const requestLatencyEWMAAlpha = 0.2

type requestLatencyTracker struct {
	mu            sync.Mutex
	latencyEWMAms float64
	initialized   bool
}

func (t *requestLatencyTracker) Observe(duration time.Duration) float64 {
	latencyMs := float64(duration) / float64(time.Millisecond)
	if latencyMs < 0 {
		latencyMs = 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.initialized {
		t.latencyEWMAms = latencyMs
		t.initialized = true
		return t.latencyEWMAms
	}

	t.latencyEWMAms = requestLatencyEWMAAlpha*latencyMs + (1-requestLatencyEWMAAlpha)*t.latencyEWMAms
	return t.latencyEWMAms
}
