package capacity

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const cpuCapacityDebugLogEnv = "CADENCE_CPU_CAPACITY_DEBUG_LOG"

var (
	cpuDebugLogMu      sync.Mutex
	cpuDebugLogPath    string
	cpuDebugLogHeader  string
	cpuDebugLogEnabled bool
	cpuDebugLogChecked bool
)

func appendCPUDebugCSV(header string, fields ...string) {
	cpuDebugLogMu.Lock()
	defer cpuDebugLogMu.Unlock()

	if !cpuDebugLogChecked {
		cpuDebugLogPath = os.Getenv(cpuCapacityDebugLogEnv)
		cpuDebugLogEnabled = cpuDebugLogPath != ""
		cpuDebugLogChecked = true
	}
	if !cpuDebugLogEnabled {
		return
	}

	if cpuDebugLogHeader == "" {
		cpuDebugLogHeader = header
	}

	file, err := os.OpenFile(cpuDebugLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return
	}
	if info.Size() == 0 {
		_, _ = file.WriteString(cpuDebugLogHeader + "\n")
	}

	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	line := strings.Join(append([]string{timestamp}, fields...), ",")
	_, _ = file.WriteString(line + "\n")
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}

// LogExecutorCPURaw records cumulative process CPU seconds sent in an executor heartbeat.
func LogExecutorCPURaw(executorID string, processCPUSeconds float64, sampleTime time.Time) {
	appendCPUDebugCSV(
		"record_type,executor_id,process_cpu_seconds,sample_unix_nanos",
		"executor_raw",
		executorID,
		formatFloat(processCPUSeconds),
		strconv.FormatInt(sampleTime.UnixNano(), 10),
	)
}

// LogCPUObservation records raw and smoothed CPU-seconds capacity observations.
func LogCPUObservation(
	executorID string,
	busyCores float64,
	rawCost float64,
	smoothedCost float64,
	load float64,
	sampleTime time.Time,
) {
	smoothedBusyCores := smoothedCost * load
	appendCPUDebugCSV(
		"record_type,executor_id,busy_cores,raw_cost,smoothed_cost,smoothed_busy_cores,load,sample_unix_nanos",
		"observation",
		executorID,
		formatFloat(busyCores),
		formatFloat(rawCost),
		formatFloat(smoothedCost),
		formatFloat(smoothedBusyCores),
		formatFloat(load),
		strconv.FormatInt(sampleTime.UnixNano(), 10),
	)
}
