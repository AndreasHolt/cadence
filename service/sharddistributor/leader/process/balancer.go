package process

import (
	"math"
	"sort"

	"github.com/uber/cadence/service/sharddistributor/store"
)

// planLoadBasedAssignment assigns unassigned shards to executors based on current load
func planLoadBasedAssignment(
	unassignedShards []string,
	loads map[string]float64,
	stats map[string]store.ShardStatistics,
	currentAssignments map[string][]string,
) map[string][]string {
	if len(unassignedShards) == 0 || len(loads) == 0 {
		return map[string][]string{}
	}

	assignment := make(map[string][]string)

	// Copy existing loads
	currentLoads := make(map[string]float64)
	currentCounts := make(map[string]int)
	for k, v := range loads {
		currentLoads[k] = v
		if currentAssignments != nil {
			currentCounts[k] = len(currentAssignments[k])
		}
	}

	// Sort shards by weight descending (heaviest first)
	shards := make([]string, len(unassignedShards))
	copy(shards, unassignedShards)
	sort.Slice(shards, func(i, j int) bool {
		return shardLoad(stats, shards[i]) > shardLoad(stats, shards[j])
	})

	// Assign each shard to executor with lowest current load
	for _, shardID := range shards {
		executorID := findLeastLoadedExecutor(currentLoads, currentCounts)
		if executorID == "" {
			continue
		}
		assignment[executorID] = append(assignment[executorID], shardID)
		currentLoads[executorID] += shardLoad(stats, shardID)
		currentCounts[executorID]++
	}

	return assignment
}

func findLeastLoadedExecutor(loads map[string]float64, counts map[string]int) string {
	if len(loads) == 0 {
		return ""
	}

	ids := make([]string, 0, len(loads))
	for id := range loads {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	minID := ids[0]
	minLoad := loads[minID]

	for _, id := range ids[1:] {
		load := loads[id]
		if load < minLoad {
			minLoad = load
			minID = id
			continue
		}
		if load == minLoad {
			if counts[id] < counts[minID] {
				minID = id
			}
		}
	}
	return minID
}

// currentAssignments already contain executor-shard mappings, so we don't need cache.
func computeExecutorLoads(
	assignments map[string][]string,
	stats map[string]store.ShardStatistics,
) map[string]float64 {
	loads := make(map[string]float64, len(assignments))

	for executorID, shardIDs := range assignments {
		load := 0.0
		for _, shardID := range shardIDs {
			if stat, ok := stats[shardID]; ok {
				load += safeLoad(stat.SmoothedLoad)
			}
		}
		loads[executorID] = load
	}

	return loads
}

func shardLoad(stats map[string]store.ShardStatistics, shardID string) float64 {
	if stats == nil {
		return 0
	}
	stat, ok := stats[shardID]
	if !ok {
		return 0
	}
	return safeLoad(stat.SmoothedLoad)
}

func safeLoad(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}
