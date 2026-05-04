package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildTraceWorkloadDeterministicEvents(t *testing.T) {
	tracePath := filepath.Join(t.TempDir(), "trace.csv")
	if err := os.WriteFile(tracePath, []byte(
		"2025-12-21 00:00:00,2,1,0\n"+
			"2025-12-21 00:00:10,1,0,1\n",
	), 0o600); err != nil {
		t.Fatalf("write trace: %v", err)
	}

	cfg := traceConfig{
		Path:              tracePath,
		Interval:          10 * time.Second,
		QPSScale:          1,
		TimeScale:         1,
		TopN:              3,
		PollerCapacityQPS: 10,
		TaskListPrefix:    "tl-",
	}

	workload, err := buildTraceWorkload(cfg)
	if err != nil {
		t.Fatalf("build trace workload: %v", err)
	}

	if len(workload.taskLists) != 3 {
		t.Fatalf("tasklists = %d, want 3", len(workload.taskLists))
	}
	if workload.taskLists[0].Name != "tl-0000" || workload.taskLists[1].Name != "tl-0001" || workload.taskLists[2].Name != "tl-0002" {
		t.Fatalf("tasklists = %+v", workload.taskLists)
	}
	if len(workload.events) != 50 {
		t.Fatalf("events = %d, want 50", len(workload.events))
	}

	first := workload.events[0]
	if first.at != 0 || first.taskList != "tl-0000" || first.workflowID != "trace-row-000000-tl-0000-seq-000000" {
		t.Fatalf("first event = %+v", first)
	}

	last := workload.events[len(workload.events)-1]
	if last.at != 19*time.Second || last.taskList != "tl-0002" || last.workflowID != "trace-row-000001-tl-0002-seq-000009" {
		t.Fatalf("last event = %+v", last)
	}
}

func TestBuildTraceWorkloadScalesQPS(t *testing.T) {
	tracePath := filepath.Join(t.TempDir(), "trace.csv")
	if err := os.WriteFile(tracePath, []byte("2025-12-21 00:00:00,10\n"), 0o600); err != nil {
		t.Fatalf("write trace: %v", err)
	}

	cfg := traceConfig{
		Path:              tracePath,
		Interval:          10 * time.Second,
		QPSScale:          0.25,
		TimeScale:         1,
		TopN:              1,
		PollerCapacityQPS: 50,
		TaskListPrefix:    "tl-",
	}

	workload, err := buildTraceWorkload(cfg)
	if err != nil {
		t.Fatalf("build trace workload: %v", err)
	}

	if len(workload.events) != 25 {
		t.Fatalf("events = %d, want 25", len(workload.events))
	}
	if got := workload.taskLists[0].Pollers; got != 1 {
		t.Fatalf("pollers = %d, want 1", got)
	}
}

func TestSelectTraceRows(t *testing.T) {
	rows := [][]float64{{1}, {2}, {3}, {4}}
	selected := selectTraceRows(rows, 1, 2)

	if len(selected) != 2 || selected[0][0] != 2 || selected[1][0] != 3 {
		t.Fatalf("selected = %+v", selected)
	}
}
