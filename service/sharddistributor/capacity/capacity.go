package capacity

import "strconv"

const (
	// GoMaxProcsMetadataKey stores the executor's current process-level GOMAXPROCS value.
	GoMaxProcsMetadataKey = "capacity.gomaxprocs"
)

// HeartbeatMetadata returns a copy of metadata extended with the V1 capacity fields.
// V1 only includes stable capacity signals (GOMAXPROCS) that do not change between
// heartbeats. This avoids unnecessary etcd metadata writes and rebalance notifications.
// Freshness of capacity data can be inferred from the executor's LastHeartbeat timestamp.
func HeartbeatMetadata(metadata map[string]string, goMaxProcs int) map[string]string {
	result := make(map[string]string, len(metadata)+1)
	for key, value := range metadata {
		result[key] = value
	}

	result[GoMaxProcsMetadataKey] = strconv.Itoa(goMaxProcs)

	return result
}

// WeightFromMetadata returns the V1 executor capacity weight parsed from heartbeat metadata.
// Missing or invalid metadata falls back to the weight of 1.
func WeightFromMetadata(metadata map[string]string) float64 {
	if len(metadata) == 0 {
		return 1
	}

	rawWeight, ok := metadata[GoMaxProcsMetadataKey]
	if !ok {
		return 1
	}

	goMaxProcs, err := strconv.Atoi(rawWeight)
	if err != nil || goMaxProcs <= 0 {
		return 1
	}

	return float64(goMaxProcs)
}

// NormalizeLoad returns a load divided by its capacity weight.
func NormalizeLoad(load, weight float64) float64 {
	if weight <= 0 {
		weight = 1
	}
	return load / weight
}
