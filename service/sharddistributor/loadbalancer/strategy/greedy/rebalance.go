package greedy

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/capacity"
	"github.com/uber/cadence/service/sharddistributor/config"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/plan"
	"github.com/uber/cadence/service/sharddistributor/store"
)

const (
	maxMissingShardStatsRatioForRebalance = 0.02
	maxStaleShardStatsRatioForRebalance   = 0.10
	minRelativeLatency                    = 0.5
	maxRelativeLatency                    = 2.0
)

// PlanRebalance returns planned shard moves for the current assignment state.
func PlanRebalance(
	cfg config.LoadBalancingGreedyConfig,
	namespace string,
	namespaceState *store.NamespaceState,
	currentAssignments map[string][]string,
	now time.Time,
	shardStatsStaleAfter time.Duration,
	metricsScope metrics.Scope,
) ([]plan.Move, error) {
	now = now.UTC()
	workingAssignments := cloneAssignments(currentAssignments)
	loads, totalLoad := computeExecutorLoads(workingAssignments, namespaceState)
	if len(loads) == 0 {
		return nil, nil
	}

	targetLoads := computeTargetLoads(loads, computeExecutorCapacityWeights(cfg.HeterogeneityMode(namespace), workingAssignments, namespaceState), totalLoad)
	totalShards := 0
	for _, shards := range currentAssignments {
		totalShards += len(shards)
	}
	moveBudget := computeMoveBudget(totalShards, cfg.MoveBudgetProportion(namespace))
	if moveBudget <= 0 {
		return nil, nil
	}
	if shouldSkipRebalanceForLoadVisibility(currentAssignments, namespaceState, now, shardStatsStaleAfter) {
		return nil, nil
	}
	moves := make([]plan.Move, 0, moveBudget)
	movedShards := make(map[string]struct{})

	// Plan multiple moves per cycle (within budget), recomputing eligibility after each move.
	// Stop early once sources/destinations are empty, i.e. imbalance is within hysteresis bands.
	for moveBudget > 0 {
		sourceExecutors, destinationExecutors := classifySourcesAndDestinations(
			loads,
			namespaceState,
			targetLoads,
			cfg.HysteresisUpperBand(namespace),
			cfg.HysteresisLowerBand(namespace),
		)

		if len(sourceExecutors) == 0 {
			break
		}

		// If we have sources but no destinations under the normal lower band,
		// allow moving to the least-loaded ACTIVE executor when imbalance is severe.
		if len(destinationExecutors) == 0 {
			if !isSevereImbalance(loads, targetLoads, cfg.SevereImbalanceRatio(namespace)) {
				break
			}
			relaxed := make(map[string]struct{})
			for executorID := range workingAssignments {
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
			shardToMove, idx, found := findShardToMove(
				workingAssignments,
				namespaceState,
				sourceExecutor,
				destExecutor,
				loads,
				targetLoads,
				movedShards,
				now,
				cfg.PerShardCooldown(namespace),
			)
			if !found {
				// No eligible shard for this source+destination (cooldown, or no beneficial move), try the next source.
				continue
			}

			if err := moveShard(workingAssignments, sourceExecutor, destExecutor, shardToMove, idx); err != nil {
				return nil, err
			}
			moves = append(moves, plan.Move{
				ShardID: shardToMove,
				From:    sourceExecutor,
				To:      destExecutor,
			})
			movedShards[shardToMove] = struct{}{}

			if metricsScope != nil {
				load := 0.0
				if stats, ok := namespaceState.ShardStats[shardToMove]; ok {
					load = stats.SmoothedLoad
				} else if report := namespaceState.Executors[sourceExecutor].ReportedShards[shardToMove]; report != nil {
					load = report.ShardLoad
				}
				metricsScope.UpdateGauge(metrics.ShardDistributorAssignLoopMovedShardLoad, load)
			}
			updateExecutorLoadsAfterMove(namespaceState, sourceExecutor, destExecutor, loads, shardToMove)
			moveBudget--
			movedThisIteration = true
			break
		}

		// No eligible shard could be moved from any source.
		if !movedThisIteration {
			break
		}
	}
	if len(moves) > 0 && metricsScope != nil {
		metricsScope.AddCounter(metrics.ShardDistributorAssignLoopLoadBasedMoves, int64(len(moves)))
	}
	return moves, nil
}

func cloneAssignments(assignments map[string][]string) map[string][]string {
	cloned := make(map[string][]string, len(assignments))
	for executorID, shardIDs := range assignments {
		clonedShards := make([]string, len(shardIDs))
		copy(clonedShards, shardIDs)
		cloned[executorID] = clonedShards
	}
	return cloned
}

func computeExecutorLoads(currentAssignments map[string][]string, state *store.NamespaceState) (map[string]float64, float64) {
	loads := make(map[string]float64, len(currentAssignments))
	total := 0.0

	for executorID, shards := range currentAssignments {
		for _, shardID := range shards {
			load := 0.0
			if stats, ok := state.ShardStats[shardID]; ok {
				load = stats.SmoothedLoad
			} else if report := state.Executors[executorID].ReportedShards[shardID]; report != nil {
				load = report.ShardLoad
			}
			loads[executorID] += load
			total += load
		}
	}

	return loads, total
}

func computeExecutorCapacityWeights(
	heterogeneityMode string,
	currentAssignments map[string][]string,
	state *store.NamespaceState,
) map[string]float64 {
	weights := make(map[string]float64, len(currentAssignments))
	for executorID := range currentAssignments {
		weights[executorID] = 1
	}
	if heterogeneityMode != config.GreedyHeterogeneityModeLatency {
		return weights
	}

	meanLatencyMs := computeMeanLatencyMs(currentAssignments, state)
	for executorID := range currentAssignments {
		weight := capacity.WeightFromMetadata(state.Executors[executorID].Metadata)
		if meanLatencyMs > 0 {
			latencyMs := capacity.LatencyEWmaMsFromMetadata(state.Executors[executorID].Metadata)
			if latencyMs > 0 {
				relativeLatency := clamp(latencyMs/meanLatencyMs, minRelativeLatency, maxRelativeLatency)
				weight = weight / math.Sqrt(relativeLatency)
			}
		}
		weights[executorID] = weight
	}

	return weights
}

func computeMeanLatencyMs(currentAssignments map[string][]string, state *store.NamespaceState) float64 {
	totalLatencyMs := 0.0
	count := 0
	for executorID := range currentAssignments {
		latencyMs := capacity.LatencyEWmaMsFromMetadata(state.Executors[executorID].Metadata)
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

func computeMoveBudget(totalShards int, proportion float64) int {
	if totalShards <= 0 || proportion <= 0 {
		return 0
	}
	return int(math.Ceil(proportion * float64(totalShards)))
}

func shouldSkipRebalanceForLoadVisibility(
	currentAssignments map[string][]string,
	state *store.NamespaceState,
	now time.Time,
	shardStatsStaleAfter time.Duration,
) bool {
	missingRatio, staleRatio := shardStatsVisibilityRatios(currentAssignments, state, now, shardStatsStaleAfter)
	return missingRatio > maxMissingShardStatsRatioForRebalance || staleRatio > maxStaleShardStatsRatioForRebalance
}

func shardStatsVisibilityRatios(
	currentAssignments map[string][]string,
	state *store.NamespaceState,
	now time.Time,
	shardStatsStaleAfter time.Duration,
) (float64, float64) {
	if state == nil {
		return 0, 0
	}

	totalAssigned := 0
	missing := 0
	stale := 0
	for _, shards := range currentAssignments {
		for _, shardID := range shards {
			totalAssigned++
			stats, ok := state.ShardStats[shardID]
			if !ok || stats.LastUpdateTime.IsZero() {
				missing++
				continue
			}
			if shardStatsStaleAfter > 0 && now.Sub(stats.LastUpdateTime) > shardStatsStaleAfter {
				stale++
			}
		}
	}
	if totalAssigned == 0 {
		return 0, 0
	}
	return float64(missing) / float64(totalAssigned), float64(stale) / float64(totalAssigned)
}

func classifySourcesAndDestinations(
	executorLoads map[string]float64,
	state *store.NamespaceState,
	targetLoads map[string]float64,
	upperBand float64,
	lowerBand float64,
) (map[string]struct{}, map[string]struct{}) {
	sources := make(map[string]struct{})
	destinations := make(map[string]struct{})

	for executorID, load := range executorLoads {
		executor := state.Executors[executorID]
		targetLoad := targetLoads[executorID]
		// Intentionally allow DRAINING executors as sources so they can shed shards
		if load > targetLoad*upperBand {
			sources[executorID] = struct{}{}
		} else if executor.Status == types.ExecutorStatusACTIVE && load < targetLoad*lowerBand {
			destinations[executorID] = struct{}{}
		}
	}

	return sources, destinations
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

func sourcesSortedByDescendingExcessLoad(sourceExecutors map[string]struct{}, executorLoads, targetLoads map[string]float64) []string {
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

func findBestDestination(destinationExecutors map[string]struct{}, executorLoads, targetLoads map[string]float64) string {
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

func findShardToMove(
	currentAssignments map[string][]string,
	state *store.NamespaceState,
	source string,
	destination string,
	executorLoads map[string]float64,
	targetLoads map[string]float64,
	movedShards map[string]struct{},
	now time.Time,
	perShardCooldown time.Duration,
) (string, int, bool) {
	bestShard := ""

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

		stats, hasStats := state.ShardStats[shard]
		if hasStats && !stats.LastMoveTime.IsZero() && perShardCooldown > 0 && now.Sub(stats.LastMoveTime) < perShardCooldown {
			continue
		}

		load := 0.0
		if hasStats {
			load = stats.SmoothedLoad
		} else if report := state.Executors[source].ReportedShards[shard]; report != nil {
			load = report.ShardLoad
		}

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

func computeBenefitOfMove(sourceLoad, sourceTargetLoad, destinationLoad, destinationTargetLoad, shardLoad float64) float64 {
	return 2*shardLoad*((sourceLoad-sourceTargetLoad)-(destinationLoad-destinationTargetLoad)) - 2*shardLoad*shardLoad
}

func moveShard(currentAssignments map[string][]string, sourceExecutor string, destExecutor string, shardID string, idx int) error {
	// Defensive fallback in case index is stale.
	if idx < 0 || idx >= len(currentAssignments[sourceExecutor]) || currentAssignments[sourceExecutor][idx] != shardID {
		idx = slices.Index(currentAssignments[sourceExecutor], shardID)
	}
	if idx == -1 {
		return fmt.Errorf("shard %s not found in source executor %s", shardID, sourceExecutor)
	}

	currentAssignments[sourceExecutor][idx] = currentAssignments[sourceExecutor][len(currentAssignments[sourceExecutor])-1]
	currentAssignments[sourceExecutor] = currentAssignments[sourceExecutor][:len(currentAssignments[sourceExecutor])-1]
	currentAssignments[destExecutor] = append(currentAssignments[destExecutor], shardID)
	return nil
}

func updateExecutorLoadsAfterMove(
	state *store.NamespaceState,
	source string,
	destination string,
	executorLoads map[string]float64,
	shardID string,
) {
	load := 0.0
	if stats, ok := state.ShardStats[shardID]; ok {
		load = stats.SmoothedLoad
	} else if report := state.Executors[source].ReportedShards[shardID]; report != nil {
		load = report.ShardLoad
	}
	executorLoads[source] -= load
	executorLoads[destination] += load
}
