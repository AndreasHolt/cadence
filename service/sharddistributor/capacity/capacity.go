package capacity

import "encoding/json"

const (
	// RuntimeMetadataKey stores runtime capacity metadata for the executor.
	RuntimeMetadataKey = "capacity.runtime"
)

// RuntimeMetadata is the compact runtime-capacity payload stored in heartbeat metadata.
type RuntimeMetadata struct {
	GoMaxProcs        int      `json:"gomaxprocs"`
	ProcessCPUSeconds *float64 `json:"process_cpu_seconds,omitempty"`
	SampleUnixNanos   *int64   `json:"sample_unix_nanos,omitempty"`
}

// HeartbeatMetadataOptions contains runtime capacity values added to heartbeat metadata.
type HeartbeatMetadataOptions struct {
	GoMaxProcs        int
	ProcessCPUSeconds float64
	HasProcessCPU     bool
	SampleUnixNanos   int64
}

// HeartbeatMetadata returns a copy of metadata extended with capacity-related
// fields that should be included in executor heartbeats.
func HeartbeatMetadata(metadata map[string]string, options HeartbeatMetadataOptions) map[string]string {
	heartbeatMetadata := make(map[string]string, len(metadata)+1)
	for key, value := range metadata {
		heartbeatMetadata[key] = value
	}

	runtimeMetadata := RuntimeMetadata{
		GoMaxProcs: options.GoMaxProcs,
	}
	if options.HasProcessCPU {
		runtimeMetadata.ProcessCPUSeconds = &options.ProcessCPUSeconds
		runtimeMetadata.SampleUnixNanos = &options.SampleUnixNanos
	}

	encoded, err := json.Marshal(runtimeMetadata)
	if err != nil {
		// RuntimeMetadata only contains primitive fields, so this should not fail.
		return heartbeatMetadata
	}
	heartbeatMetadata[RuntimeMetadataKey] = string(encoded)

	return heartbeatMetadata
}

// WeightFromMetadata returns the executor capacity weight parsed from heartbeat
// metadata. Missing or invalid metadata falls back to the neutral weight of 1.
func WeightFromMetadata(metadata map[string]string) float64 {
	runtimeMetadata, ok := RuntimeMetadataFromMetadata(metadata)
	if !ok || runtimeMetadata.GoMaxProcs <= 0 {
		return 1
	}

	return float64(runtimeMetadata.GoMaxProcs)
}

// RuntimeMetadataFromMetadata parses runtime capacity metadata from heartbeat metadata.
func RuntimeMetadataFromMetadata(metadata map[string]string) (RuntimeMetadata, bool) {
	if len(metadata) == 0 {
		return RuntimeMetadata{}, false
	}

	rawRuntimeMetadata, ok := metadata[RuntimeMetadataKey]
	if !ok {
		return RuntimeMetadata{}, false
	}

	var runtimeMetadata RuntimeMetadata
	if err := json.Unmarshal([]byte(rawRuntimeMetadata), &runtimeMetadata); err != nil {
		return RuntimeMetadata{}, false
	}

	return runtimeMetadata, true
}
