package process

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/capacity"
	"github.com/uber/cadence/service/sharddistributor/store"
)

const (
	minRelativeLatency = 0.8
	maxRelativeLatency = 1.25
)

func (p *namespaceProcessor) rebalanceGreedyBySmoothedLoad(
	namespaceState *store.NamespaceState,
	currentAssignments map[string][]string,
	metricsScope metrics.Scope,
) (bool, error) {
	loads, totalLoad := computeExecutorLoads(currentAssignments, namespaceState)
	if len(loads) == 0 {
		return false, nil
	}

	executorCapacityWeights := computeExecutorCapacityWeights(currentAssignments, namespaceState)
	targetLoads := computeTargetLoads(loads, executorCapacityWeights, totalLoad)
	totalShards := 0
	for _, shards := range currentAssignments {
		totalShards += len(shards)
	}
	moveBudget := computeMoveBudget(totalShards, p.cfg.LoadBalance.MoveBudgetProportion)
	if moveBudget <= 0 {
		return false, nil
	}
	shardsMoved := false
	movesPlanned := 0
	movedShards := make(map[string]struct{})
	now := p.timeSource.Now().UTC()

	// Plan multiple moves per cycle (within budget), recomputing eligibility after each move.
	// Stop early once sources/destinations are empty, i.e. imbalance is within hysteresis bands.
	for moveBudget > 0 {
		sourceExecutors, destinationExecutors := classifySourcesAndDestinations(
			loads,
			namespaceState,
			targetLoads,
			p.cfg.LoadBalance.HysteresisUpperBand,
			p.cfg.LoadBalance.HysteresisLowerBand,
		)

		if len(sourceExecutors) == 0 {
			break
		}

		// If we have sources but no destinations under the normal lower band,
		// allow moving to the least-loaded ACTIVE executor when imbalance is severe.
		if len(destinationExecutors) == 0 {
			if !isSevereImbalance(loads, targetLoads, p.cfg.LoadBalance.SevereImbalanceRatio) {
				break
			}
			relaxed := make(map[string]struct{})
			for executorID := range currentAssignments {
				if namespaceState.Executors[executorID].Status == types.ExecutorStatusACTIVE {
					relaxed[executorID] = struct{}{}
				}
			}
			if len(relaxed) == 0 {
				break
			}
			destinationExecutors = relaxed
		}

		sources := sourcesSortedByDescendingExcessLoad(sourceExecutors, loads, targetLoads)

		destExecutor := findBestDestination(destinationExecutors, loads, targetLoads)
		if destExecutor == "" {
			break
		}

		// Try sources in priority order to find a shard that is not in per-shard cooldown.
		// movedThisIteration tracks whether we actually performed a move in this iteration.
		// If no source has an eligible shard (e.g., all are cooling down), we stop early.
		movedThisIteration := false
		for _, sourceExecutor := range sources {
			if sourceExecutor == destExecutor {
				continue
			}
			shardToMove, idx, found := p.findShardToMove(
				currentAssignments,
				namespaceState,
				sourceExecutor,
				destExecutor,
				loads,
				targetLoads,
				movedShards,
				now,
			)
			if !found {
				// No eligible shard for this source+destination (cooldown, or no beneficial move), try the next source.
				continue
			}

			if err := p.moveShard(currentAssignments, sourceExecutor, destExecutor, shardToMove, idx); err != nil {
				return false, err
			}
			movesPlanned++
			shardsMoved = true
			movedShards[shardToMove] = struct{}{}

			if metricsScope != nil {
				metricsScope.UpdateGauge(metrics.ShardDistributorAssignLoopMovedShardLoad, namespaceState.ShardStats[shardToMove].SmoothedLoad)
			}
			p.updateExecutorLoadsAfterMove(namespaceState, sourceExecutor, destExecutor, loads, shardToMove)
			moveBudget--
			movedThisIteration = true
			break
		}

		// No eligible shard could be moved from any source.
		if !movedThisIteration {
			break
		}
	}
	if movesPlanned > 0 && metricsScope != nil {
		metricsScope.AddCounter(metrics.ShardDistributorAssignLoopLoadBasedMoves, int64(movesPlanned))
	}
	return shardsMoved, nil
}

func computeExecutorLoads(currentAssignments map[string][]string, namespaceState *store.NamespaceState) (map[string]float64, float64) {
	loads := make(map[string]float64, len(currentAssignments))
	total := 0.0

	for executorID, shards := range currentAssignments {
		for _, shardID := range shards {
			stats, ok := namespaceState.ShardStats[shardID]
			load := 0.0
			if ok {
				load = stats.SmoothedLoad
			}
			loads[executorID] += load
			total += load
		}
	}

	return loads, total
}

func computeExecutorCapacityWeights(currentAssignments map[string][]string, namespaceState *store.NamespaceState) map[string]float64 {
	executorCapacityWeights := make(map[string]float64, len(currentAssignments))
	meanLatencyMs := computeMeanLatencyMs(currentAssignments, namespaceState)

	for executorID := range currentAssignments {
		weight := capacity.WeightFromMetadata(namespaceState.Executors[executorID].Metadata)
		if meanLatencyMs > 0 {
			latencyMs := capacity.LatencyEWmaMsFromMetadata(namespaceState.Executors[executorID].Metadata)
			if latencyMs > 0 {
				relativeLatency := clamp(latencyMs/meanLatencyMs, minRelativeLatency, maxRelativeLatency)
				weight = weight / math.Sqrt(relativeLatency)
			}
		}
		executorCapacityWeights[executorID] = weight
	}
	return executorCapacityWeights
}

func computeMeanLatencyMs(currentAssignments map[string][]string, namespaceState *store.NamespaceState) float64 {
	totalLatencyMs := 0.0
	count := 0
	for executorID := range currentAssignments {
		latencyMs := capacity.LatencyEWmaMsFromMetadata(namespaceState.Executors[executorID].Metadata)
		if latencyMs <= 0 {
			continue
		}
		totalLatencyMs += latencyMs
		count++
	}
	if count == 0 {
		return 0
	}
	return totalLatencyMs / float64(count)
}

func clamp(value, minValue, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func computeTargetLoads(executorLoads map[string]float64, executorCapacityWeights map[string]float64, totalLoad float64) map[string]float64 {
	targetLoads := make(map[string]float64, len(executorLoads))

	totalWeight := 0.0
	for executorID := range executorLoads {
		weight := executorCapacityWeights[executorID]
		if weight <= 0 {
			weight = 1
		}
		totalWeight += weight
	}

	if totalWeight <= 0 {
		return targetLoads
	}

	for executorID := range executorLoads {
		weight := executorCapacityWeights[executorID]
		if weight <= 0 {
			weight = 1
		}
		targetLoads[executorID] = (weight / totalWeight) * totalLoad
	}

	return targetLoads
}

func isSevereImbalance(executorLoads map[string]float64, targetLoads map[string]float64, severeImbalanceRatio float64) bool {
	if severeImbalanceRatio <= 0 {
		return false
	}

	for executorID, load := range executorLoads {
		targetLoad := targetLoads[executorID]
		if targetLoad <= 0 {
			continue
		}
		if load/targetLoad >= severeImbalanceRatio {
			return true
		}
	}
	return false
}

// classifySourcesAndDestinations returns the source and destination executor sets for rebalancing.
func classifySourcesAndDestinations(
	executorLoads map[string]float64,
	namespaceState *store.NamespaceState,
	targetLoads map[string]float64,
	upperBand float64,
	lowerBand float64,
) (map[string]struct{}, map[string]struct{}) {
	sources := make(map[string]struct{})
	destinations := make(map[string]struct{})

	for executorID, load := range executorLoads {
		executor := namespaceState.Executors[executorID]
		targetLoad := targetLoads[executorID]
		if load > targetLoad*upperBand {
			sources[executorID] = struct{}{}
		} else if executor.Status == types.ExecutorStatusACTIVE && load < targetLoad*lowerBand {
			destinations[executorID] = struct{}{}
		}
	}

	return sources, destinations
}

// sourcesSortedByDescendingExcessLoad orders sources by descending overload above
// target so we prefer to move shards away from the most over-capacity executors first.
func sourcesSortedByDescendingExcessLoad(sourceExecutors map[string]struct{}, executorLoads map[string]float64, targetLoads map[string]float64) []string {
	sources := make([]string, 0, len(sourceExecutors))
	for executorID := range sourceExecutors {
		sources = append(sources, executorID)
	}

	slices.SortFunc(sources, func(a, b string) int {
		la := executorLoads[a] - targetLoads[a]
		lb := executorLoads[b] - targetLoads[b]
		switch {
		case la > lb:
			return -1
		case la < lb:
			return 1
		default:
			return 0
		}
	})

	return sources
}

func computeMoveBudget(totalShards int, proportion float64) int {
	if totalShards <= 0 || proportion <= 0 {
		return 0
	}
	return int(math.Ceil(proportion * float64(totalShards)))
}

func findBestDestination(destinationExecutors map[string]struct{}, executorLoads map[string]float64, targetLoads map[string]float64) string {
	maxDeficit := -math.MaxFloat64
	bestExecutor := ""
	for executor := range destinationExecutors {
		deficit := targetLoads[executor] - executorLoads[executor]
		if deficit > maxDeficit {
			maxDeficit = deficit
			bestExecutor = executor
		}
	}
	return bestExecutor
}

// findShardToMove returns the best shard to move from source to destination.
func (p *namespaceProcessor) findShardToMove(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	executorLoads map[string]float64,
	targetLoads map[string]float64,
	movedShards map[string]struct{},
	now time.Time,
) (string, int, bool) {
	bestShard := ""
	perShardCooldown := p.cfg.LoadBalance.PerShardCooldown

	sourceLoad := executorLoads[source]
	destLoad := executorLoads[destination]
	sourceTargetLoad := targetLoads[source]
	destinationTargetLoad := targetLoads[destination]
	idx := -1

	bestBenefit := 0.0
	for i, shard := range currentAssignments[source] {
		if _, ok := movedShards[shard]; ok {
			continue
		}

		stats, ok := namespaceState.ShardStats[shard]
		if !ok {
			continue
		}
		if perShardCooldown > 0 && !stats.LastMoveTime.IsZero() && now.Sub(stats.LastMoveTime) < perShardCooldown {
			continue
		}

		load := stats.SmoothedLoad

		benefit := computeBenefitOfMove(sourceLoad, sourceTargetLoad, destLoad, destinationTargetLoad, load)
		if benefit <= 0 {
			continue
		}
		if benefit > bestBenefit {
			bestBenefit = benefit
			bestShard = shard
			idx = i
		}
	}

	return bestShard, idx, bestShard != ""
}

// computeBenefitOfMove returns the expected reduction in sum of squared error
// around capacity-proportional target loads if we move a shard from source to
// destination. A positive value means the move improves overall load balance.
func computeBenefitOfMove(sourceLoad, sourceTargetLoad, destinationLoad, destinationTargetLoad, shardLoad float64) float64 {
	return 2*shardLoad*((sourceLoad-sourceTargetLoad)-(destinationLoad-destinationTargetLoad)) - 2*shardLoad*shardLoad
}

func (p *namespaceProcessor) moveShard(currentAssignments map[string][]string, sourceExecutor string, destExecutor string, shardID string, idx int) error {
	// defensive fallback in case index is stale
	if idx < 0 || idx >= len(currentAssignments[sourceExecutor]) || currentAssignments[sourceExecutor][idx] != shardID {
		idx = slices.Index(currentAssignments[sourceExecutor], shardID)
	}
	//
	if idx == -1 {
		return fmt.Errorf("shard %s not found in source executor %s", shardID, sourceExecutor)
	}

	// Remove shard from source.
	currentAssignments[sourceExecutor][idx] = currentAssignments[sourceExecutor][len(currentAssignments[sourceExecutor])-1]
	currentAssignments[sourceExecutor] = currentAssignments[sourceExecutor][:len(currentAssignments[sourceExecutor])-1]

	// Add shard to destination.
	currentAssignments[destExecutor] = append(currentAssignments[destExecutor], shardID)
	return nil
}

func (p *namespaceProcessor) updateExecutorLoadsAfterMove(
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	executorLoads map[string]float64,
	shardID string,
) {
	stats, ok := namespaceState.ShardStats[shardID]
	if !ok {
		return
	}
	executorLoads[source] -= stats.SmoothedLoad
	executorLoads[destination] += stats.SmoothedLoad
}
