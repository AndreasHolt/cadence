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
	loadBalanceStopReasonNoLoad               = "no_load"
	loadBalanceStopReasonMoveBudgetZero       = "move_budget_zero"
	loadBalanceStopReasonMoveBudgetExhausted  = "move_budget_exhausted"
	loadBalanceStopReasonNoSources            = "no_sources"
	loadBalanceStopReasonNoDestinations       = "no_destinations_not_severe"
	loadBalanceStopReasonNoActiveDestinations = "no_active_destinations"
	loadBalanceStopReasonNoDestinationExec    = "no_destination_executor"
	loadBalanceStopReasonNoEligibleShard      = "no_eligible_shard"
)

func (p *namespaceProcessor) loadBalance(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	deletedShards map[string]store.ShardState,
	metricsScope metrics.Scope,
) (bool, error) {
	if metricsScope != nil {
		metricsScope.AddCounter(metrics.ShardDistributorLoadBalanceCycles, 1)
	}

	loads, _ := computeExecutorLoads(currentAssignments, namespaceState)
	if len(loads) == 0 {
		if metricsScope != nil {
			metricsScope.Tagged(metrics.ReasonTag(loadBalanceStopReasonNoLoad)).
				IncCounter(metrics.ShardDistributorLoadBalanceStopReason)
		}
		return false, nil
	}

	weights := computeExecutorWeights(currentAssignments, namespaceState)
	targetLoads := computeTargetLoads(loads, weights)
	allShards := getShards(p.namespaceCfg, namespaceState, deletedShards)
	moveBudget := computeMoveBudget(len(allShards), p.cfg.LoadBalance.MoveBudgetProportion)
	shardsMoved := false
	movesPlanned := 0
	now := p.timeSource.Now().UTC()
	stopReason := loadBalanceStopReasonMoveBudgetExhausted
	initialSourceCount := 0
	initialDestinationCount := 0

	if moveBudget <= 0 {
		if metricsScope != nil {
			metricsScope.Tagged(metrics.ReasonTag(loadBalanceStopReasonMoveBudgetZero)).
				IncCounter(metrics.ShardDistributorLoadBalanceStopReason)
		}
		return false, nil
	}

	initialSources, initialDestinations := classifySourcesAndDestinations(
		loads,
		targetLoads,
		p.cfg.LoadBalance.HysteresisUpperBand,
		p.cfg.LoadBalance.HysteresisLowerBand,
	)
	initialSourceCount = len(initialSources)
	initialDestinationCount = len(initialDestinations)

	// Plan multiple moves per cycle (within budget), recomputing eligibility after each move.
	// Stop early once sources/destinations are empty, i.e. imbalance is within hysteresis bands.
	for moveBudget > 0 {
		sourceExecutors, destinationExecutors := classifySourcesAndDestinations(
			loads,
			targetLoads,
			p.cfg.LoadBalance.HysteresisUpperBand,
			p.cfg.LoadBalance.HysteresisLowerBand,
		)

		if len(sourceExecutors) == 0 {
			stopReason = loadBalanceStopReasonNoSources
			break
		}

		// Escape hatch: if we have sources but no destinations under the normal lower band,
		// allow moving to the least-loaded ACTIVE executor when imbalance is severe.
		if len(destinationExecutors) == 0 {
			if !isSevereImbalance(loads, targetLoads, p.cfg.LoadBalance.SevereImbalanceRatio) {
				stopReason = loadBalanceStopReasonNoDestinations
				break
			}
			relaxed := make(map[string]struct{})
			for executorID := range currentAssignments {
				if namespaceState.Executors[executorID].Status == types.ExecutorStatusACTIVE {
					relaxed[executorID] = struct{}{}
				}
			}
			if len(relaxed) == 0 {
				stopReason = loadBalanceStopReasonNoActiveDestinations
				break
			}
			destinationExecutors = relaxed
		}

		sources := sourcesSortedByDescendingExcessLoad(sourceExecutors, loads, targetLoads)

		destExecutor := findBestDestination(destinationExecutors, loads, targetLoads)
		if destExecutor == "" {
			stopReason = loadBalanceStopReasonNoDestinationExec
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

			p.updateExecutorLoadsAfterMove(namespaceState, sourceExecutor, destExecutor, loads, shardToMove)
			moveBudget--
			movedThisIteration = true
			break
		}

		// No eligible shard could be moved from any source.
		if !movedThisIteration {
			stopReason = loadBalanceStopReasonNoEligibleShard
			break
		}
	}

	if metricsScope != nil {
		if movesPlanned > 0 {
			metricsScope.AddCounter(metrics.ShardDistributorLoadBalanceMoves, int64(movesPlanned))
		}
		metricsScope.UpdateGauge(metrics.ShardDistributorLoadBalanceSourceExecutorsInitial, float64(initialSourceCount))
		metricsScope.UpdateGauge(metrics.ShardDistributorLoadBalanceDestinationExecutorsInitial, float64(initialDestinationCount))
		metricsScope.Tagged(metrics.ReasonTag(stopReason)).
			IncCounter(metrics.ShardDistributorLoadBalanceStopReason)
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

func computeExecutorWeights(currentAssignments map[string][]string, namespaceState *store.NamespaceState) map[string]float64 {
	weights := make(map[string]float64, len(currentAssignments))
	for executorID := range currentAssignments {
		weights[executorID] = capacity.WeightFromMetadata(namespaceState.Executors[executorID].Metadata)
	}
	return weights
}

// computeTargetLoads returns the ideal load per executor in a perfectly balanced
// cluster, proportional to capacity weight. When all weights are equal this is
// simply totalLoad / numExecutors (the mean).
func computeTargetLoads(executorLoads, executorWeights map[string]float64) map[string]float64 {
	targets := make(map[string]float64, len(executorLoads))

	totalLoad := 0.0
	totalWeight := 0.0
	for executorID, load := range executorLoads {
		totalLoad += load
		weight := executorWeights[executorID]
		if weight <= 0 {
			weight = 1
		}
		totalWeight += weight
	}

	if totalWeight <= 0 {
		return targets
	}

	for executorID := range executorLoads {
		weight := executorWeights[executorID]
		if weight <= 0 {
			weight = 1
		}
		targets[executorID] = (weight / totalWeight) * totalLoad
	}

	return targets
}

// isSevereImbalance reports whether any executor's load exceeds its target by
// at least the given ratio (e.g. 1.5 means 50% above target).
func isSevereImbalance(executorLoads, targetLoads map[string]float64, severeImbalanceRatio float64) bool {
	if severeImbalanceRatio <= 0 {
		return false
	}

	for executorID, load := range executorLoads {
		target := targetLoads[executorID]
		if target <= 0 {
			continue
		}
		if load/target >= severeImbalanceRatio {
			return true
		}
	}
	return false
}

// classifySourcesAndDestinations returns the source and destination executor sets for rebalancing.
func classifySourcesAndDestinations(
	executorLoads map[string]float64,
	targetLoads map[string]float64,
	upperBand float64,
	lowerBand float64,
) (map[string]struct{}, map[string]struct{}) {
	sources := make(map[string]struct{})
	destinations := make(map[string]struct{})

	for executorID, load := range executorLoads {
		targetLoad := targetLoads[executorID]
		if load > targetLoad*upperBand {
			sources[executorID] = struct{}{}
		} else if load < targetLoad*lowerBand {
			destinations[executorID] = struct{}{}
		}
	}

	return sources, destinations
}

// sourcesSortedByDescendingExcessLoad orders sources by descending overload above target.
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

func computeMoveBudget(totalShards int, proportion float64) int {
	if totalShards <= 0 || proportion <= 0 {
		return 0
	}
	return int(math.Ceil(proportion * float64(totalShards)))
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

// findShardToMove returns the best shard to move from source to destination.
func (p *namespaceProcessor) findShardToMove(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	executorLoads map[string]float64,
	targetLoads map[string]float64,
	now time.Time,
) (string, int, bool) {
	bestShard := ""
	perShardCooldown := p.cfg.LoadBalance.PerShardCooldown
	benefitGatingDisabled := p.cfg.LoadBalance.DisableBenefitGating

	sourceLoad := executorLoads[source]
	destLoad := executorLoads[destination]
	sourceTarget := targetLoads[source]
	destTarget := targetLoads[destination]
	idx := -1

	// If benefitGatingDisabled is true we allow moves that are not beneficial according to computeBenefitOfMove.
	if benefitGatingDisabled {
		bestLoad := -1.0
		for i, shard := range currentAssignments[source] {
			stats, ok := namespaceState.ShardStats[shard]
			if !ok {
				continue
			}
			if perShardCooldown > 0 && !stats.LastMoveTime.IsZero() && now.Sub(stats.LastMoveTime) < perShardCooldown {
				continue
			}
			if stats.SmoothedLoad > bestLoad {
				bestLoad = stats.SmoothedLoad
				bestShard = shard
				idx = i
			}
		}
		return bestShard, idx, bestShard != ""
	}

	bestBenefit := 0.0
	for i, shard := range currentAssignments[source] {
		stats, ok := namespaceState.ShardStats[shard]
		if !ok {
			continue
		}
		if perShardCooldown > 0 && !stats.LastMoveTime.IsZero() && now.Sub(stats.LastMoveTime) < perShardCooldown {
			continue
		}

		load := stats.SmoothedLoad

		benefit := computeBenefitOfMove(sourceLoad, sourceTarget, destLoad, destTarget, load)
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

// computeBenefitOfMove returns the reduction in sum of squared deviations around
// weighted targets if we move a shard from source to destination.
// A positive value means the move improves overall load balance.
func computeBenefitOfMove(sourceLoad, sourceTarget, destLoad, destTarget, shardLoad float64) float64 {
	w := shardLoad
	return 2*w*((sourceLoad-sourceTarget)-(destLoad-destTarget)) - 2*w*w
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
