package handler

import (
	"math"
	"testing"
	"time"
)

func TestRequestLatencyTrackerObserveAtUsesTimeBasedEWMA(t *testing.T) {
	tracker := &requestLatencyTracker{}
	start := time.Unix(0, 0)

	if got := tracker.observeAt(start, 100*time.Millisecond); got != 100 {
		t.Fatalf("initial latency = %v, want 100", got)
	}

	got := tracker.observeAt(start.Add(requestLatencyEWMATau), 200*time.Millisecond)
	alpha := 1 - math.Exp(-1)
	want := alpha*200 + (1-alpha)*100
	if math.Abs(got-want) > 0.000001 {
		t.Fatalf("latency after one tau = %v, want %v", got, want)
	}
}

func TestRequestLatencyTrackerObserveAtClampsNegativeLatency(t *testing.T) {
	tracker := &requestLatencyTracker{}
	start := time.Unix(0, 0)

	tracker.observeAt(start, 100*time.Millisecond)
	got := tracker.observeAt(start.Add(requestLatencyEWMATau), -time.Second)
	alpha := 1 - math.Exp(-1)
	want := (1 - alpha) * 100
	if math.Abs(got-want) > 0.000001 {
		t.Fatalf("latency after negative sample = %v, want %v", got, want)
	}
}
