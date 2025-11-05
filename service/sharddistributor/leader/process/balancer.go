package process

import (
	"fmt"
	"math"
	"sort"

	"github.com/uber/cadence/service/sharddistributor/store"
)

type SortCriteria int

const CoolDownTime = 60

const (
	None SortCriteria = iota
	LeastLoad
	MostLoad
	// ...
)

type Configuration struct {
	Cooldown bool
	Sorting  SortCriteria
}

// planLoadBasedAssignment assigns unassigned shards to executors based on current load
func assignUnassignedShards(
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
		executorID, err := findLeastLoadedExecutor(currentLoads, currentCounts)
		if err != nil {
			continue
		}
		assignment[executorID] = append(assignment[executorID], shardID)
		currentLoads[executorID] += shardLoad(stats, shardID)
		currentCounts[executorID]++
	}

	return assignment
}

func (p *namespaceProcessor) redistributeToEmptyExecutors(
	loads map[string]float64,
	stats map[string]store.ShardStatistics,
	assignments map[string][]string,
) (map[string][]string, map[string]float64) {
	if len(assignments) == 0 {
		return nil, loads
	}

	emptyExecutors := make([]string, 0)
	for executorID, shards := range assignments {
		if len(shards) == 0 {
			emptyExecutors = append(emptyExecutors, executorID)
		}
	}
	if len(emptyExecutors) == 0 {
		return nil, loads
	}

	type shardCandidate struct {
		executor string
		shardID  string
		weight   float64
	}
	var donors []shardCandidate
	for executorID, shards := range assignments {
		if len(shards) == 0 {
			continue
		}
		elligbleShards, err := p.FindElligbleShards(shards, stats, Configuration{Cooldown: true, Sorting: None})
		if err != nil {
			p.logger.Error(fmt.sprintf("error in find elligbleshards: %s", err.Error()))
			return nil, loads
		}
		for _, shardID := range elligbleShards {
			donors = append(donors, shardCandidate{
				executor: executorID,
				shardID:  shardID,
				weight:   shardLoad(stats, shardID),
			})
		}
	}
	if len(donors) == 0 {
		return nil, loads
	}

	sort.Slice(donors, func(i, j int) bool {
		if donors[i].weight == donors[j].weight {
			if donors[i].executor == donors[j].executor {
				return donors[i].shardID < donors[j].shardID
			}
			return donors[i].executor < donors[j].executor
		}
		return donors[i].weight > donors[j].weight
	})

	sort.Strings(emptyExecutors)

	steals := make(map[string][]string, len(emptyExecutors))
	updatedLoads := make(map[string]float64, len(loads))
	for k, v := range loads {
		updatedLoads[k] = v
	}

	used := make(map[string]struct{}, len(emptyExecutors))
	donorIdx := 0
	for _, target := range emptyExecutors {
		if donorIdx >= len(donors) {
			break
		}
		var candidate shardCandidate
		for donorIdx < len(donors) {
			candidate = donors[donorIdx]
			donorIdx++
			if candidate.executor != target {
				break
			}
		}
		if candidate.executor == target {
			continue
		}

		if _, taken := used[candidate.shardID]; taken {
			continue
		}
		used[candidate.shardID] = struct{}{}

		steals[target] = append(steals[target], candidate.shardID)
		assignments[candidate.executor] = removeShard(assignments[candidate.executor], candidate.shardID)
		updatedLoads[candidate.executor] -= candidate.weight
		updatedLoads[target] += candidate.weight
	}

	return steals, updatedLoads
}

func (p *namespaceProcessor) FindElligbleShards(shardIDs []string, stats map[string]store.ShardStatistics, config Configuration) ([]string, error) {
	var elligbleShards []string
	for _, shardID := range shardIDs {
		stat, ok := stats[shardID]
		if !ok {
			return nil, fmt.Errorf("cound not find stats for shard id: %s", shardID)
		}
		delta := p.timeSource.Now().Unix() - stat.LastMoveTime
		if delta > CoolDownTime {
			elligbleShards = append(elligbleShards, shardID)
		}
	}
	return elligbleShards, nil
}

func findLeastLoadedExecutor(loads map[string]float64, counts map[string]int) (string, error) {
	if len(loads) == 0 {
		return "", fmt.Errorf("empty loads array")
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
	return minID, nil
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
			load += shardLoad(stats, shardID)
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

func removeShard(shards []string, target string) []string {
	for i, id := range shards {
		if id == target {
			return append(shards[:i], shards[i+1:]...)
		}
	}
	return shards
}
