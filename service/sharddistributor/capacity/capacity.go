package capacity

import "strconv"

const (
	// GoMaxProcsMetadataKey stores the executor's current process-level GOMAXPROCS value.
	GoMaxProcsMetadataKey = "capacity.gomaxprocs"
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
