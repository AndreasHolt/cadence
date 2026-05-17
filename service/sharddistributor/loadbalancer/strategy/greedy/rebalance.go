package greedy

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
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
	minRelativeCPUCost                    = 0.5
	maxRelativeCPUCost                    = 2.0
)

type moveCandidate struct {
	shardID         string
	from            string
	to              string
	assignmentIndex int
}

// PlanRebalance returns planned shard moves for the current assignment state.
func PlanRebalance(
	cfg config.LoadBalancingGreedyConfig,
	namespace string,
	namespaceState *store.NamespaceState,
	currentAssignments map[string][]string,
	now time.Time,
	shardStatsStaleAfter time.Duration,
	logger log.Logger,
	metricsScope metrics.Scope,
	cpuObservationState ...*CPUObservationState,
) ([]plan.Move, error) {
	now = now.UTC()
	workingAssignments := cloneAssignments(currentAssignments)
	loads, totalLoad, ok := computeExecutorLoads(workingAssignments, namespaceState)
	if !ok {
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

	// Plan multiple moves per cycle (within budget), recomputing eligibility after each move.
	// Stop early once sources/destinations are empty, i.e. imbalance is within hysteresis bands.
	for moveBudget > 0 {
		move, moved, err := planAndApplyNextMove(
			cfg, namespace, namespaceState, workingAssignments, loads, targetLoads, movedShards, now,
			perShardCooldown, totalLoad, moveScoringMode, movePenaltyCoefficient, logger, metricsScope,
		)
		if err != nil {
			return nil, err
		}
		if !moved {
			break
		}

		moves = append(moves, move)
		moveBudget--
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

func computeExecutorLoads(currentAssignments map[string][]string, state *store.NamespaceState) (map[string]float64, float64, bool) {
	loads := make(map[string]float64, len(currentAssignments))
	total := 0.0

	for executorID, shards := range currentAssignments {
		for _, shardID := range shards {
			stats, ok := state.ShardStats[shardID]
			if ok {
				loads[executorID] += stats.SmoothedLoad
				total += stats.SmoothedLoad
			} else if report := state.Executors[executorID].ReportedShards[shardID]; report != nil {
				loads[executorID] += report.ShardLoad
				total += report.ShardLoad
			}
		}
	}
	if len(loads) == 0 {
		return loads, 0, false
	}

	return loads, total, true
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
		relativeCost := clamp(cost/averageCPUCost, minRelativeCPUCost, maxRelativeCPUCost)
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

// planAndApplyNextMove attempts to plan one beneficial move and applies it to
// the in-memory working assignments, executor loads, and moved-shard set. It
// returns moved=false when no eligible move is available and the caller should
// stop the rebalance pass.
func planAndApplyNextMove(
	cfg config.LoadBalancingGreedyConfig,
	namespace string,
	namespaceState *store.NamespaceState,
	workingAssignments map[string][]string,
	loads map[string]float64,
	targetLoads map[string]float64,
	movedShards map[string]struct{},
	now time.Time,
	perShardCooldown time.Duration,
	totalLoad float64,
	moveScoringMode string,
	movePenaltyCoefficient float64,
	logger log.Logger,
	metricsScope metrics.Scope,
) (plan.Move, bool, error) {
	sourceExecutors, destinationExecutors := classifySourcesAndDestinations(
		loads,
		namespaceState,
		targetLoads,
		cfg.HysteresisUpperBand(namespace),
		cfg.HysteresisLowerBand(namespace),
	)
	if len(sourceExecutors) == 0 {
		return plan.Move{}, false, nil
	}

	destinationExecutor, ok := selectDestinationExecutor(
		destinationExecutors,
		workingAssignments,
		namespaceState,
		loads,
		targetLoads,
		cfg.SevereImbalanceRatio(namespace),
	)
	if !ok {
		return plan.Move{}, false, nil
	}

	candidate, found := findNextMoveCandidate(
		sourceExecutors,
		destinationExecutor,
		workingAssignments,
		namespaceState,
		loads,
		targetLoads,
		movedShards,
		now,
		perShardCooldown,
		totalLoad,
		moveScoringMode,
		movePenaltyCoefficient,
	)
	if !found {
		return plan.Move{}, false, nil
	}

	if err := applyMoveCandidate(workingAssignments, candidate); err != nil {
		return plan.Move{}, false, err
	}

	movedShards[candidate.shardID] = struct{}{}
	updateExecutorLoadsAfterMove(namespaceState, candidate.from, candidate.to, loads, candidate.shardID)

	shardLoad := shardLoad(namespaceState, candidate.from, candidate.shardID)
	logGreedyMove(logger, loads, plan.Move{
		ShardID: candidate.shardID,
		From:    candidate.from,
		To:      candidate.to,
	}, shardLoad)
	if metricsScope != nil {
		metricsScope.UpdateGauge(metrics.ShardDistributorAssignLoopMovedShardLoad, shardLoad)
	}

	return plan.Move{
		ShardID: candidate.shardID,
		From:    candidate.from,
		To:      candidate.to,
	}, true, nil
}

// selectDestinationExecutor picks the least-loaded destination executor. If
// there are no destination executors, it falls back to all ACTIVE executors only
// when the namespace is severely imbalanced.
func selectDestinationExecutor(
	destinationExecutors []string,
	workingAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	loads map[string]float64,
	targetLoads map[string]float64,
	severeImbalanceRatio float64,
) (string, bool) {
	if len(destinationExecutors) == 0 {
		if !isSevereImbalance(loads, targetLoads, severeImbalanceRatio) {
			return "", false
		}
		allActiveExecutors := make([]string, 0, len(workingAssignments))
		for executorID := range workingAssignments {
			if namespaceState.Executors[executorID].Status == types.ExecutorStatusACTIVE {
				allActiveExecutors = append(allActiveExecutors, executorID)
			}
		}
		if len(allActiveExecutors) == 0 {
			return "", false
		}
		destinationExecutors = allActiveExecutors
	}

	return findBestDestination(destinationExecutors, loads, targetLoads)
}

// findNextMoveCandidate searches sources by descending load and returns the
// first eligible source/shard pair for the destination.
func findNextMoveCandidate(
	sourceExecutors []string,
	destinationExecutor string,
	workingAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	loads map[string]float64,
	targetLoads map[string]float64,
	movedShards map[string]struct{},
	now time.Time,
	perShardCooldown time.Duration,
	totalLoad float64,
	moveScoringMode string,
	movePenaltyCoefficient float64,
) (moveCandidate, bool) {
	sortByDescendingLoad(sourceExecutors, loads)
	for _, sourceExecutor := range sourceExecutors {
		if sourceExecutor == destinationExecutor {
			continue
		}
		shardID, idx, found := findBestShardForMove(
			workingAssignments,
			namespaceState,
			sourceExecutor,
			destinationExecutor,
			loads,
			targetLoads,
			movedShards,
			now,
			perShardCooldown,
			totalLoad,
			moveScoringMode,
			movePenaltyCoefficient,
		)
		if !found {
			// No eligible shard for this source+destination (cooldown, or no beneficial move), try the next source.
			continue
		}

		return moveCandidate{
			shardID:         shardID,
			from:            sourceExecutor,
			to:              destinationExecutor,
			assignmentIndex: idx,
		}, true
	}

	return moveCandidate{}, false
}

func classifySourcesAndDestinations(
	executorLoads map[string]float64,
	state *store.NamespaceState,
	targetLoads map[string]float64,
	upperBand float64,
	lowerBand float64,
) ([]string, []string) {
	sources := make([]string, 0)
	destinations := make([]string, 0)

	for executorID, load := range executorLoads {
		executor := state.Executors[executorID]
		targetLoad := targetLoads[executorID]
		// Intentionally allow DRAINING executors as sources so they can shed shards
		if load > targetLoad*upperBand {
			sources = append(sources, executorID)
		} else if executor.Status == types.ExecutorStatusACTIVE && load < targetLoad*lowerBand {
			destinations = append(destinations, executorID)
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

func findBestDestination(destinationExecutors []string, executorLoads, targetLoads map[string]float64) (string, bool) {
	maxDeficit := -math.MaxFloat64
	bestExecutor := ""
	for _, executor := range destinationExecutors {
		deficit := targetLoads[executor] - executorLoads[executor]
		if deficit > maxDeficit {
			maxDeficit = deficit
			bestExecutor = executor
		}
	}
	return bestExecutor, bestExecutor != ""
}

func sortByDescendingLoad(executors []string, executorLoads map[string]float64) {
	slices.SortFunc(executors, func(a, b string) int {
		return cmp.Compare(executorLoads[b], executorLoads[a])
	})
}

func findBestShardForMove(
	currentAssignments map[string][]string,
	state *store.NamespaceState,
	source string,
	destination string,
	executorLoads map[string]float64,
	targetLoads map[string]float64,
	movedShards map[string]struct{},
	now time.Time,
	perShardCooldown time.Duration,
	totalLoad float64,
	moveScoringMode string,
	movePenaltyCoefficient float64,
) (string, int, bool) {
	bestShard := ""

	sourceLoad := executorLoads[source]
	destLoad := executorLoads[destination]
	sourceTargetLoad := targetLoads[source]
	destinationTargetLoad := targetLoads[destination]
	idx := -1

	bestScore := 0.0
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

		benefit := computeCapacityNormalizedBenefitOfMove(sourceLoad, sourceTargetLoad, destLoad, destinationTargetLoad, load)
		if benefit <= 0 {
			continue
		}

		penaltyCoefficient := 0.0
		if moveScoringMode == config.GreedyMoveScoringModeCostAware {
			penaltyCoefficient = movePenaltyCoefficient
		}
		cost := computeMoveCost(totalLoad, load, penaltyCoefficient)
		score := benefit - cost
		if score > bestScore {
			bestScore = score
			bestShard = shard
			idx = i
		}
	}

	return bestShard, idx, bestShard != ""
}

// computeBenefitOfMove computes the reduction in sum of squared deviations from
// target loads when moving shardLoad from source to destination.
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

func computeMoveCost(totalLoad, shardLoad, penaltyCoefficient float64) float64 {
	if totalLoad <= 0 || shardLoad <= 0 {
		return 0
	}
	return (shardLoad / totalLoad) * penaltyCoefficient
}

// applyMoveCandidate applies a planned move to the in-memory assignment state.
func applyMoveCandidate(currentAssignments map[string][]string, candidate moveCandidate) error {
	if candidate.assignmentIndex < 0 || candidate.assignmentIndex >= len(currentAssignments[candidate.from]) {
		return fmt.Errorf("candidate assignment index out of range for shard %s on source executor %s", candidate.shardID, candidate.from)
	}
	if currentAssignments[candidate.from][candidate.assignmentIndex] != candidate.shardID {
		return fmt.Errorf("candidate assignment index mismatch for shard %s on source executor %s", candidate.shardID, candidate.from)
	}

	currentAssignments[candidate.from][candidate.assignmentIndex] = currentAssignments[candidate.from][len(currentAssignments[candidate.from])-1]
	currentAssignments[candidate.from] = currentAssignments[candidate.from][:len(currentAssignments[candidate.from])-1]
	currentAssignments[candidate.to] = append(currentAssignments[candidate.to], candidate.shardID)
	return nil
}

func updateExecutorLoadsAfterMove(
	state *store.NamespaceState,
	source string,
	destination string,
	executorLoads map[string]float64,
	shardID string,
) {
	load := shardLoad(state, source, shardID)
	executorLoads[source] -= load
	executorLoads[destination] += load
}

func shardLoad(state *store.NamespaceState, source string, shardID string) float64 {
	if stats, ok := state.ShardStats[shardID]; ok {
		return stats.SmoothedLoad
	}
	if report := state.Executors[source].ReportedShards[shardID]; report != nil {
		return report.ShardLoad
	}
	return 0
}

func logGreedyMove(logger log.Logger, loads map[string]float64, move plan.Move, shardLoad float64) {
	sourceLoadBefore := loads[move.From] + shardLoad
	destinationLoadBefore := loads[move.To] - shardLoad
	logger.Info("Greedy load-based shard move",
		tag.ShardKey(move.ShardID),
		tag.ShardExecutor(move.From),
		tag.Dynamic("destination_executor", move.To),
		tag.ShardLoad(fmt.Sprintf("%f", shardLoad)),
		tag.Dynamic("source_executor_load_before", sourceLoadBefore),
		tag.Dynamic("destination_executor_load_before", destinationLoadBefore),
	)
}
