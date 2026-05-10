package capacity

import "strconv"

const (
	// GoMaxProcsMetadataKey stores the executor's current process-level GOMAXPROCS value.
	GoMaxProcsMetadataKey = "capacity.gomaxprocs"
	// LatencyEWmaMsMetadataKey stores the executor's smoothed request latency in milliseconds.
	LatencyEWmaMsMetadataKey = "pressure.latency_ewma_ms"
)

// HeartbeatMetadata returns a copy of metadata extended with capacity-related
// fields that should be included in executor heartbeats.
func HeartbeatMetadata(metadata map[string]string, goMaxProcs int) map[string]string {
	heartbeatMetadata := make(map[string]string, len(metadata)+1)
	for key, value := range metadata {
		heartbeatMetadata[key] = value
	}

	heartbeatMetadata[GoMaxProcsMetadataKey] = strconv.Itoa(goMaxProcs)

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
