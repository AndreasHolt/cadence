package main

import (
	"testing"
	"time"
)

func TestStatsSnapshotReportsWindowLatencyPercentiles(t *testing.T) {
	st := newStats()
	start := time.Unix(0, 0)

	st.started.Add(3)
	st.polled.Add(3)
	for i, latency := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 900 * time.Millisecond} {
		workflowID := string(rune('a' + i))
		st.recordWorkflowStart(workflowID, start)
		st.completed.Add(1)
		st.recordWorkflowCompletion(workflowID, start.Add(latency))
	}

	snapshot, current := st.snapshot(start.Add(10*time.Second), start, 10*time.Second, cumulativeCounters{})

	if snapshot.WindowCompleted != 3 {
		t.Fatalf("WindowCompleted = %d, want 3", snapshot.WindowCompleted)
	}
	if snapshot.WindowCompletedRPS != 0.3 {
		t.Fatalf("WindowCompletedRPS = %v, want 0.3", snapshot.WindowCompletedRPS)
	}
	if snapshot.WindowLatencyP50Millis != 200 {
		t.Fatalf("WindowLatencyP50Millis = %v, want 200", snapshot.WindowLatencyP50Millis)
	}
	if snapshot.WindowLatencyP95Millis != 900 {
		t.Fatalf("WindowLatencyP95Millis = %v, want 900", snapshot.WindowLatencyP95Millis)
	}

	next, _ := st.snapshot(start.Add(20*time.Second), start, 10*time.Second, current)
	if next.WindowLatencySamples != 0 {
		t.Fatalf("WindowLatencySamples = %d, want 0 after snapshot drains samples", next.WindowLatencySamples)
	}
}

func TestStatsSnapshotPrunesOldTrackedWorkflows(t *testing.T) {
	st := newStats()
	start := time.Unix(0, 0)

	st.recordWorkflowStart("old", start)
	st.recordWorkflowStart("fresh", start.Add(9*time.Minute))

	snapshot, _ := st.snapshot(start.Add(11*time.Minute), start, 10*time.Second, cumulativeCounters{})

	if snapshot.PrunedTrackedIncomplete != 1 {
		t.Fatalf("PrunedTrackedIncomplete = %d, want 1", snapshot.PrunedTrackedIncomplete)
	}
	if snapshot.TrackedIncomplete != 1 {
		t.Fatalf("TrackedIncomplete = %d, want 1", snapshot.TrackedIncomplete)
	}
}
