package greedy

import (
	"cmp"
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
	minRelativeLatency = 0.5
	maxRelativeLatency = 2.0
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
	cpuObservationState ...*CPUObservationState,
) ([]plan.Move, error) {
	now = now.UTC()
	workingAssignments := cloneAssignments(currentAssignments)
	loads, totalLoad := computeExecutorLoads(workingAssignments, namespaceState)
	if len(loads) == 0 {
		return nil, nil
	}

	var cpuState *CPUObservationState
	if len(cpuObservationState) > 0 {
		cpuState = cpuObservationState[0]
	}
	if cpuState != nil {
		cpuState.SetSmoothingTau(cfg.CPUSecondsSmoothingTau(namespace))
	}
	targetLoads := computeTargetLoads(loads, computeExecutorCapacityWeights(cfg.HeterogeneityMode(namespace), workingAssignments, namespaceState, loads, cpuState), totalLoad)
	averageExecutorTarget := computeAverageExecutorTarget(targetLoads)
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
	moveScoringMode := cfg.MoveScoringMode(namespace)
	movePenaltyCoefficient := cfg.MovePenaltyCoefficient(namespace)
	perShardCooldown := cfg.PerShardCooldown(namespace)
	var singleMoves, swapMoves, multiMoves int

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
			penaltyCoefficient := 0.0
			if moveScoringMode == config.GreedyMoveScoringModeCostAware {
				penaltyCoefficient = movePenaltyCoefficient
			}
			shardsToMove, found := findShardsToMove(
				workingAssignments,
				namespaceState,
				sourceExecutor,
				destExecutor,
				loads,
				targetLoads,
				movedShards,
				now,
				perShardCooldown,
				averageExecutorTarget,
				penaltyCoefficient,
				cfg.EnableSwap(namespace),
				cfg.EnableMultiMove(namespace),
				moveBudget,
			)
			if !found {
				// No eligible shard for this source+destination (cooldown, or no beneficial move), try the next source.
				continue
			}

			if err := moveShards(workingAssignments, shardsToMove); err != nil {
				return nil, err
			}
			moves = append(moves, shardsToMove...)
			for _, m := range shardsToMove {
				movedShards[m.ShardID] = struct{}{}
			}

			switch classifyPlannedMoveType(shardsToMove) {
			case plannedMoveTypeSwap:
				swapMoves++
			case plannedMoveTypeMulti:
				multiMoves++
			default:
				singleMoves += len(shardsToMove)
			}

			if metricsScope != nil {
				for _, m := range shardsToMove {
					load := 0.0
					if stats, ok := namespaceState.ShardStats[m.ShardID]; ok {
						load = stats.SmoothedLoad
					} else if report := namespaceState.Executors[m.From].ReportedShards[m.ShardID]; report != nil {
						load = report.ShardLoad
					}
					metricsScope.UpdateGauge(metrics.ShardDistributorAssignLoopMovedShardLoad, load)
					// Preserve fractional load in an integer counter.
					metricsScope.AddCounter(metrics.ShardDistributorAssignLoopMovedShardLoadTotal, int64(load*1000))
				}
			}
			updateExecutorLoadsAfterMoves(namespaceState, loads, shardsToMove)
			moveBudget -= len(shardsToMove)
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
	if singleMoves > 0 && metricsScope != nil {
		metricsScope.AddCounter(metrics.ShardDistributorAssignLoopLoadBasedSingleMoves, int64(singleMoves))
	}
	if swapMoves > 0 && metricsScope != nil {
		metricsScope.AddCounter(metrics.ShardDistributorAssignLoopLoadBasedSwapMoves, int64(swapMoves))
	}
	if multiMoves > 0 && metricsScope != nil {
		metricsScope.AddCounter(metrics.ShardDistributorAssignLoopLoadBasedMultiMoves, int64(multiMoves))
	}
	return moves, nil
}

const (
	plannedMoveTypeSingle = iota
	plannedMoveTypeSwap
	plannedMoveTypeMulti
)

func classifyPlannedMoveType(moves []plan.Move) int {
	if len(moves) == 2 && moves[0].From == moves[1].To && moves[0].To == moves[1].From {
		return plannedMoveTypeSwap
	}
	if len(moves) >= 2 {
		return plannedMoveTypeMulti
	}
	return plannedMoveTypeSingle
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
	loads map[string]float64,
	cpuObservationState *CPUObservationState,
) map[string]float64 {
	weights := make(map[string]float64, len(currentAssignments))
	for executorID := range currentAssignments {
		weights[executorID] = 1
	}
	switch heterogeneityMode {
	case config.GreedyHeterogeneityModeLatency:
		return computeLatencyAdjustedWeights(currentAssignments, state, weights)
	case config.GreedyHeterogeneityModeCPUSeconds:
		return computeCPUSecondsAdjustedWeights(currentAssignments, state, loads, cpuObservationState, weights)
	default:
		return weights
	}
}

func computeLatencyAdjustedWeights(
	currentAssignments map[string][]string,
	state *store.NamespaceState,
	weights map[string]float64,
) map[string]float64 {
	meanLatencyMs := computeMeanLatencyMs(currentAssignments, state)
	for executorID := range currentAssignments {
		weight := capacity.WeightFromMetadata(state.Executors[executorID].Metadata)
		if meanLatencyMs > 0 {
			latencyMs := capacity.LatencyEWmaMsFromMetadata(state.Executors[executorID].Metadata)
			if latencyMs > 0 {
				relativeLatency := clamp(latencyMs/meanLatencyMs, minRelativeLatency, maxRelativeLatency)
				weight = weight / relativeLatency
			}
		}
		weights[executorID] = weight
	}

	return weights
}

func computeCPUSecondsAdjustedWeights(
	currentAssignments map[string][]string,
	state *store.NamespaceState,
	loads map[string]float64,
	cpuObservationState *CPUObservationState,
	weights map[string]float64,
) map[string]float64 {
	for executorID := range currentAssignments {
		weights[executorID] = capacity.WeightFromMetadata(state.Executors[executorID].Metadata)
	}
	if cpuObservationState == nil {
		return weights
	}

	cpuCosts := cpuObservationState.updateExecutorCPUCostObservations(state, loads)
	totalCPUCost := 0.0
	validCount := 0
	for _, cost := range cpuCosts {
		if cost <= 0 {
			continue
		}
		if math.IsNaN(cost) || math.IsInf(cost, 0) {
			continue
		}
		totalCPUCost += cost
		validCount++
	}
	if validCount == 0 {
		return weights
	}

	averageCPUCost := totalCPUCost / float64(validCount)
	for executorID := range currentAssignments {
		cost := cpuCosts[executorID]
		if cost <= 0 {
			cost = averageCPUCost
		}
		relativeCost := cost / averageCPUCost
		weights[executorID] = weights[executorID] / relativeCost
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

func computeAverageExecutorTarget(targetLoads map[string]float64) float64 {
	if len(targetLoads) == 0 {
		return 0
	}
	sum := 0.0
	for _, targetLoad := range targetLoads {
		sum += targetLoad
	}
	return sum / float64(len(targetLoads))
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

type shardInfo struct {
	id   string
	load float64
}

func findShardsToMove(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	executorLoads map[string]float64,
	targetLoads map[string]float64,
	movedShards map[string]struct{},
	now time.Time,
	perShardCooldown time.Duration,
	averageExecutorTarget float64,
	movePenaltyCoefficient float64,
	enableSwap bool,
	enableMultiMove bool,
	remainingMoveBudget int,
) ([]plan.Move, bool) {
	sourceLoad := executorLoads[source]
	destLoad := executorLoads[destination]
	sourceTargetLoad := targetLoads[source]
	destTargetLoad := targetLoads[destination]

	bestMoves, bestScore := findSingleShard(
		currentAssignments,
		namespaceState,
		source,
		destination,
		sourceLoad,
		destLoad,
		sourceTargetLoad,
		destTargetLoad,
		movedShards,
		perShardCooldown,
		now,
		averageExecutorTarget,
		movePenaltyCoefficient,
	)
	if bestScore <= 0 {
		bestMoves = nil
	}

	if enableMultiMove {
		multiMoves, multiScore := findMultiShards(
			currentAssignments,
			namespaceState,
			source,
			destination,
			sourceLoad,
			destLoad,
			sourceTargetLoad,
			destTargetLoad,
			movedShards,
			perShardCooldown,
			now,
			averageExecutorTarget,
			movePenaltyCoefficient,
		)
		if len(multiMoves) > 0 && len(multiMoves) <= remainingMoveBudget && multiScore > bestScore {
			bestMoves = multiMoves
			bestScore = multiScore
		}
	}

	if enableSwap && remainingMoveBudget >= 2 {
		swapMoves, swapScore := findSwapShards(
			currentAssignments,
			namespaceState,
			source,
			destination,
			sourceLoad,
			destLoad,
			sourceTargetLoad,
			destTargetLoad,
			movedShards,
			perShardCooldown,
			now,
			averageExecutorTarget,
			movePenaltyCoefficient,
		)
		if swapScore > bestScore {
			bestMoves = swapMoves
			bestScore = swapScore
		}
	}

	if bestScore <= 0 {
		return nil, false
	}
	return bestMoves, true
}

func findSingleShard(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	sourceLoad float64,
	destLoad float64,
	sourceTargetLoad float64,
	destTargetLoad float64,
	movedShards map[string]struct{},
	perShardCooldown time.Duration,
	now time.Time,
	averageExecutorTarget float64,
	movePenaltyCoefficient float64,
) ([]plan.Move, float64) {
	bestShard := ""
	bestScore := 0.0

	for _, shard := range currentAssignments[source] {
		if _, ok := movedShards[shard]; ok {
			continue
		}

		stats, hasStats := namespaceState.ShardStats[shard]
		if hasStats && !stats.LastMoveTime.IsZero() && perShardCooldown > 0 && now.Sub(stats.LastMoveTime) < perShardCooldown {
			continue
		}

		load := 0.0
		if hasStats {
			load = stats.SmoothedLoad
		} else if report := namespaceState.Executors[source].ReportedShards[shard]; report != nil {
			load = report.ShardLoad
		}

		benefit := computeCapacityNormalizedBenefitOfMove(sourceLoad, sourceTargetLoad, destLoad, destTargetLoad, load)
		if benefit <= 0 {
			continue
		}

		cost := computeMoveCost(averageExecutorTarget, load, movePenaltyCoefficient)
		score := benefit - cost
		if score > bestScore {
			bestScore = score
			bestShard = shard
		}
	}

	if bestShard == "" {
		return nil, 0
	}
	return []plan.Move{{ShardID: bestShard, From: source, To: destination}}, bestScore
}

func findMultiShards(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	sourceLoad float64,
	destLoad float64,
	sourceTargetLoad float64,
	destTargetLoad float64,
	movedShards map[string]struct{},
	perShardCooldown time.Duration,
	now time.Time,
	averageExecutorTarget float64,
	movePenaltyCoefficient float64,
) ([]plan.Move, float64) {
	var eligibleShards []shardInfo
	for _, shardID := range currentAssignments[source] {
		if _, ok := movedShards[shardID]; ok {
			continue
		}

		stats, hasStats := namespaceState.ShardStats[shardID]
		if hasStats && !stats.LastMoveTime.IsZero() && perShardCooldown > 0 && now.Sub(stats.LastMoveTime) < perShardCooldown {
			continue
		}

		load := 0.0
		if hasStats {
			load = stats.SmoothedLoad
		} else if report := namespaceState.Executors[source].ReportedShards[shardID]; report != nil {
			load = report.ShardLoad
		}
		if load <= 0 {
			continue
		}

		eligibleShards = append(eligibleShards, shardInfo{
			id:   shardID,
			load: load,
		})
	}

	slices.SortFunc(eligibleShards, func(a, b shardInfo) int {
		la, lb := a.load, b.load
		if la > lb {
			return -1
		} else if la < lb {
			return 1
		}
		return 0
	})

	idealLoad := optimalCapacityNormalizedMove(sourceLoad, sourceTargetLoad, destLoad, destTargetLoad)
	if idealLoad <= 0 {
		return nil, 0
	}

	remainingLoad := idealLoad
	selectedMoves := make([]plan.Move, 0, len(eligibleShards))
	for _, s := range eligibleShards {
		if s.load < remainingLoad {
			selectedMoves = append(selectedMoves, plan.Move{ShardID: s.id, From: source, To: destination})
			remainingLoad -= s.load
		}
	}

	if len(selectedMoves) == 0 {
		return nil, 0
	}

	movedLoad := idealLoad - remainingLoad
	benefit := computeCapacityNormalizedBenefitOfMove(sourceLoad, sourceTargetLoad, destLoad, destTargetLoad, movedLoad)
	if benefit <= 0 {
		return nil, 0
	}
	cost := computeMoveCost(averageExecutorTarget, movedLoad, movePenaltyCoefficient)
	score := benefit - cost
	if score <= 0 {
		return nil, 0
	}
	return selectedMoves, score
}

func findSwapShards(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	sourceLoad float64,
	destLoad float64,
	sourceTargetLoad float64,
	destTargetLoad float64,
	movedShards map[string]struct{},
	perShardCooldown time.Duration,
	now time.Time,
	averageExecutorTarget float64,
	movePenaltyCoefficient float64,
) ([]plan.Move, float64) {
	var eligibleShardsSource []shardInfo
	for _, shardID := range currentAssignments[source] {
		if _, ok := movedShards[shardID]; ok {
			continue
		}

		stats, ok := namespaceState.ShardStats[shardID]
		if !ok {
			continue
		}
		if perShardCooldown > 0 && !stats.LastMoveTime.IsZero() && now.Sub(stats.LastMoveTime) < perShardCooldown {
			continue
		}
		eligibleShardsSource = append(eligibleShardsSource, shardInfo{
			id:   shardID,
			load: stats.SmoothedLoad,
		})
	}

	var eligibleShardsDestination []shardInfo
	for _, shardID := range currentAssignments[destination] {
		if _, ok := movedShards[shardID]; ok {
			continue
		}

		stats, ok := namespaceState.ShardStats[shardID]
		if !ok {
			continue
		}
		if perShardCooldown > 0 && !stats.LastMoveTime.IsZero() && now.Sub(stats.LastMoveTime) < perShardCooldown {
			continue
		}
		eligibleShardsDestination = append(eligibleShardsDestination, shardInfo{
			id:   shardID,
			load: stats.SmoothedLoad,
		})
	}

	slices.SortFunc(eligibleShardsSource, func(a, b shardInfo) int {
		la, lb := a.load, b.load
		if la > lb {
			return -1
		} else if la < lb {
			return 1
		}
		return 0
	})

	idealNetMove := costAdjustedNormalizedMove(sourceLoad, sourceTargetLoad, destLoad, destTargetLoad, averageExecutorTarget, movePenaltyCoefficient)
	bestScore := 0.0
	var bestMoves []plan.Move

	for _, dShard := range eligibleShardsDestination {
		searchTarget := idealNetMove + dShard.load

		idx, _ := slices.BinarySearchFunc(eligibleShardsSource, searchTarget, func(s shardInfo, target float64) int {
			return cmp.Compare(target, s.load)
		})

		// The swap score is concave in the net transfer and peaks at searchTarget,
		// so the best source shard is one of the two straddling the insertion point:
		// idx-1 (load just above the target) and idx (load just at or below it).
		for i := idx - 1; i <= idx; i++ {
			if i < 0 || i >= len(eligibleShardsSource) {
				continue
			}
			sShard := eligibleShardsSource[i]
			actualMove := sShard.load - dShard.load
			if actualMove <= 0 {
				continue
			}
			benefit := computeCapacityNormalizedBenefitOfMove(sourceLoad, sourceTargetLoad, destLoad, destTargetLoad, actualMove)
			if benefit <= 0 {
				continue
			}
			cost := computeMoveCost(averageExecutorTarget, sShard.load+dShard.load, movePenaltyCoefficient)
			score := benefit - cost
			if score > bestScore {
				bestScore = score
				bestMoves = []plan.Move{
					{ShardID: sShard.id, From: source, To: destination},
					{ShardID: dShard.id, From: destination, To: source},
				}
			}
		}
	}

	if bestScore <= 0 {
		return nil, 0
	}
	return bestMoves, bestScore
}

func optimalCapacityNormalizedMove(sourceLoad, sourceTargetLoad, destLoad, destTargetLoad float64) float64 {
	if sourceTargetLoad <= 0 || destTargetLoad <= 0 {
		return (sourceLoad - destLoad) / 2
	}
	sourceDeviation := (sourceLoad - sourceTargetLoad) / (sourceTargetLoad * sourceTargetLoad)
	destDeviation := (destLoad - destTargetLoad) / (destTargetLoad * destTargetLoad)
	denominator := 1/(sourceTargetLoad*sourceTargetLoad) + 1/(destTargetLoad*destTargetLoad)
	return (sourceDeviation - destDeviation) / denominator
}

// costAdjustedNormalizedMove returns the net load transfer that maximizes the
// capacity-normalized swap score (benefit minus cost). The normalized benefit is a
// downward parabola in the net transfer that peaks at optimalCapacityNormalizedMove
// with curvature 1/T_s^2 + 1/T_d^2, while the move cost is linear in the net transfer
// with slope movePenaltyCoefficient/averageExecutorTarget. Their difference is therefore a parabola
// whose peak is shifted toward smaller moves by a constant that depends only on the two
// executors, so the search target can account for cost directly.
func costAdjustedNormalizedMove(sourceLoad, sourceTargetLoad, destLoad, destTargetLoad, averageExecutorTarget, movePenaltyCoefficient float64) float64 {
	ideal := optimalCapacityNormalizedMove(sourceLoad, sourceTargetLoad, destLoad, destTargetLoad)
	if sourceTargetLoad <= 0 || destTargetLoad <= 0 || averageExecutorTarget <= 0 || movePenaltyCoefficient <= 0 {
		return ideal
	}
	curvature := 1/(sourceTargetLoad*sourceTargetLoad) + 1/(destTargetLoad*destTargetLoad)
	shift := movePenaltyCoefficient / (2 * averageExecutorTarget * curvature)
	return ideal - shift
}

func destinationsSortedByDescendingDeficit(destinationExecutors map[string]struct{}, executorLoads, targetLoads map[string]float64) []string {
	destinations := make([]string, 0, len(destinationExecutors))
	for executorID := range destinationExecutors {
		destinations = append(destinations, executorID)
	}

	slices.SortFunc(destinations, func(a, b string) int {
		da := targetLoads[a] - executorLoads[a]
		db := targetLoads[b] - executorLoads[b]
		switch {
		case da > db:
			return -1
		case da < db:
			return 1
		default:
			return 0
		}
	})

	return destinations
}

func computeBenefitOfMove(sourceLoad, sourceTargetLoad, destinationLoad, destinationTargetLoad, shardLoad float64) float64 {
	return 2*shardLoad*((sourceLoad-sourceTargetLoad)-(destinationLoad-destinationTargetLoad)) - 2*shardLoad*shardLoad
}

func computeCapacityNormalizedBenefitOfMove(
	sourceLoad,
	sourceTargetLoad,
	destinationLoad,
	destinationTargetLoad,
	shardLoad float64,
) float64 {
	scoreBefore := normalizedSSE(sourceLoad, sourceTargetLoad) + normalizedSSE(destinationLoad, destinationTargetLoad)
	scoreAfter := normalizedSSE(sourceLoad-shardLoad, sourceTargetLoad) + normalizedSSE(destinationLoad+shardLoad, destinationTargetLoad)
	return scoreBefore - scoreAfter
}

func normalizedSSE(load, targetLoad float64) float64 {
	if targetLoad <= 0 {
		return 0
	}
	normalizedDeviation := load/targetLoad - 1
	return normalizedDeviation * normalizedDeviation
}

func computeMoveCost(averageExecutorTarget, shardLoad, penaltyCoefficient float64) float64 {
	if averageExecutorTarget <= 0 || shardLoad <= 0 {
		return 0
	}
	return (shardLoad / averageExecutorTarget) * penaltyCoefficient
}

func moveShards(currentAssignments map[string][]string, moves []plan.Move) error {
	for _, move := range moves {
		idx := slices.Index(currentAssignments[move.From], move.ShardID)
		if idx == -1 {
			return fmt.Errorf("shard %s not found in source executor %s", move.ShardID, move.From)
		}

		currentAssignments[move.From][idx] = currentAssignments[move.From][len(currentAssignments[move.From])-1]
		currentAssignments[move.From] = currentAssignments[move.From][:len(currentAssignments[move.From])-1]
		currentAssignments[move.To] = append(currentAssignments[move.To], move.ShardID)
	}
	return nil
}

func updateExecutorLoadsAfterMoves(
	state *store.NamespaceState,
	executorLoads map[string]float64,
	moves []plan.Move,
) {
	for _, move := range moves {
		load := 0.0
		if stats, ok := state.ShardStats[move.ShardID]; ok {
			load = stats.SmoothedLoad
		} else if report := state.Executors[move.From].ReportedShards[move.ShardID]; report != nil {
			load = report.ShardLoad
		}
		executorLoads[move.From] -= load
		executorLoads[move.To] += load
	}
}
