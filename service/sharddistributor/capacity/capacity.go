package capacity

import (
	"math"
	"strconv"
	"time"
)

const (
	// GoMaxProcsMetadataKey stores the executor's current process-level GOMAXPROCS value.
	GoMaxProcsMetadataKey = "capacity.gomaxprocs"
	// LatencyEWmaMsMetadataKey stores the executor's smoothed request latency in milliseconds.
	LatencyEWmaMsMetadataKey = "pressure.latency_ewma_ms"
	// ProcessCPUSecondsMetadataKey stores cumulative process CPU seconds for the executor.
	ProcessCPUSecondsMetadataKey = "capacity.process_cpu_seconds"
	// SampleUnixNanosMetadataKey stores the wall-clock sample time for process CPU seconds.
	SampleUnixNanosMetadataKey = "capacity.sample_unix_nanos"
)

// HeartbeatMetadataOptions contains runtime capacity values added to heartbeat metadata.
type HeartbeatMetadataOptions struct {
	GoMaxProcs        int
	ProcessCPUSeconds float64
	HasProcessCPU     bool
	SampleTime        time.Time
}

// HeartbeatMetadata returns a copy of metadata extended with capacity-related
// fields that should be included in executor heartbeats.
func HeartbeatMetadata(metadata map[string]string, goMaxProcs int) map[string]string {
	return HeartbeatMetadataWithOptions(metadata, HeartbeatMetadataOptions{
		GoMaxProcs: goMaxProcs,
	})
}

// HeartbeatMetadataWithOptions returns a copy of metadata extended with capacity-related
// fields that should be included in executor heartbeats.
func HeartbeatMetadataWithOptions(metadata map[string]string, options HeartbeatMetadataOptions) map[string]string {
	heartbeatMetadata := make(map[string]string, len(metadata)+3)
	for key, value := range metadata {
		heartbeatMetadata[key] = value
	}

	heartbeatMetadata[GoMaxProcsMetadataKey] = strconv.Itoa(options.GoMaxProcs)
	if options.HasProcessCPU && !options.SampleTime.IsZero() && options.ProcessCPUSeconds >= 0 && !math.IsNaN(options.ProcessCPUSeconds) && !math.IsInf(options.ProcessCPUSeconds, 0) {
		heartbeatMetadata[ProcessCPUSecondsMetadataKey] = strconv.FormatFloat(options.ProcessCPUSeconds, 'f', 6, 64)
		heartbeatMetadata[SampleUnixNanosMetadataKey] = strconv.FormatInt(options.SampleTime.UnixNano(), 10)
	}

	return heartbeatMetadata
}

// WeightFromMetadata returns the executor capacity weight parsed from heartbeat
// metadata. Missing or invalid metadata falls back to the neutral weight of 1.
func WeightFromMetadata(metadata map[string]string) float64 {
	if len(metadata) == 0 {
		return 1
	}

	rawGoMaxProcs, ok := metadata[GoMaxProcsMetadataKey]
	if !ok {
		return 1
	}

	goMaxProcs, err := strconv.Atoi(rawGoMaxProcs)
	if err != nil || goMaxProcs <= 0 {
		return 1
	}

	return float64(goMaxProcs)
}

// LatencyEWmaMsFromMetadata returns the executor's smoothed request latency in milliseconds.
// Missing or invalid metadata falls back to 0, which callers should treat as "no signal".
func LatencyEWmaMsFromMetadata(metadata map[string]string) float64 {
	if len(metadata) == 0 {
		return 0
	}

	rawLatency, ok := metadata[LatencyEWmaMsMetadataKey]
	if !ok {
		return 0
	}

	latencyMs, err := strconv.ParseFloat(rawLatency, 64)
	if err != nil || latencyMs < 0 {
		return 0
	}

	return latencyMs
}

// ProcessCPUSampleFromMetadata parses cumulative process CPU seconds and sample time
// from heartbeat metadata.
func ProcessCPUSampleFromMetadata(metadata map[string]string) (float64, time.Time, bool) {
	if len(metadata) == 0 {
		return 0, time.Time{}, false
	}

	rawCPUSeconds, ok := metadata[ProcessCPUSecondsMetadataKey]
	if !ok {
		return 0, time.Time{}, false
	}
	cpuSeconds, err := strconv.ParseFloat(rawCPUSeconds, 64)
	if err != nil || cpuSeconds < 0 || math.IsNaN(cpuSeconds) || math.IsInf(cpuSeconds, 0) {
		return 0, time.Time{}, false
	}

	rawSampleTime, ok := metadata[SampleUnixNanosMetadataKey]
	if !ok {
		return 0, time.Time{}, false
	}
	sampleUnixNanos, err := strconv.ParseInt(rawSampleTime, 10, 64)
	if err != nil {
		return 0, time.Time{}, false
	}

	return cpuSeconds, time.Unix(0, sampleUnixNanos).UTC(), true
}
