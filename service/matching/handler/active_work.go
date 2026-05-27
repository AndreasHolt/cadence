package handler

import (
	"math"
	"sync"
	"time"
)

const activeWorkEWMATau = time.Minute

type activeWorkTracker struct {
	mu          sync.Mutex
	ewma        float64
	lastUpdate  time.Time
	initialized bool
}

func (t *activeWorkTracker) ObserveAt(now time.Time, activeWork int64) float64 {
	value := float64(activeWork)
	if value < 0 {
		value = 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.initialized {
		t.ewma = value
		t.lastUpdate = now
		t.initialized = true
		return t.ewma
	}

	dt := now.Sub(t.lastUpdate)
	if dt < 0 {
		dt = 0
	}
	alpha := 1 - math.Exp(-dt.Seconds()/activeWorkEWMATau.Seconds())
	t.ewma = alpha*value + (1-alpha)*t.ewma
	t.lastUpdate = now
	return t.ewma
}
