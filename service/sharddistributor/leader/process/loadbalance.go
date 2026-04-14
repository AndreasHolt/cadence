package process

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
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
	loads, totalLoad := computeExecutorLoads(currentAssignments, namespaceState)
	if len(loads) == 0 {
		p.logLoadBalanceSkipped(loadBalanceStopReasonNoLoad, 0, 0, 0)
		return false, nil
	}

	meanLoad := totalLoad / float64(len(loads))
	allShards := getShards(p.namespaceCfg, namespaceState, deletedShards)
	moveBudget := computeMoveBudget(len(allShards), p.cfg.LoadBalance.MoveBudgetProportion)
	if moveBudget <= 0 {
		p.logLoadBalanceSkipped(loadBalanceStopReasonMoveBudgetZero, meanLoad, 0, 0)
		return false, nil
	}

	initialSources, initialDestinations := classifySourcesAndDestinations(
		loads,
		namespaceState,
		meanLoad,
		p.cfg.LoadBalance.HysteresisUpperBand,
		p.cfg.LoadBalance.HysteresisLowerBand,
	)

	shardsMoved := false
	movesPlanned := 0
	now := p.timeSource.Now().UTC()
	stopReason := loadBalanceStopReasonMoveBudgetExhausted

	for moveBudget > 0 {
		sourceExecutors, destinationExecutors := classifySourcesAndDestinations(
			loads,
			namespaceState,
			meanLoad,
			p.cfg.LoadBalance.HysteresisUpperBand,
			p.cfg.LoadBalance.HysteresisLowerBand,
		)

		if len(sourceExecutors) == 0 {
			stopReason = loadBalanceStopReasonNoSources
			break
		}

		if len(destinationExecutors) == 0 {
			if !isSevereImbalance(loads, meanLoad, p.cfg.LoadBalance.SevereImbalanceRatio) {
				stopReason = loadBalanceStopReasonNoDestinations
				break
			}
			destinationExecutors = activeExecutorSet(currentAssignments, namespaceState)
			if len(destinationExecutors) == 0 {
				stopReason = loadBalanceStopReasonNoActiveDestinations
				break
			}
		}

		destExecutor := findBestDestination(destinationExecutors, loads)
		if destExecutor == "" {
			stopReason = loadBalanceStopReasonNoDestinationExec
			break
		}

		movedThisIteration := false
		for _, sourceExecutor := range sourcesSortedByDescendingLoad(sourceExecutors, loads) {
			if sourceExecutor == destExecutor {
				continue
			}

			shardToMove, idx, found := p.findShardToMove(
				currentAssignments,
				namespaceState,
				sourceExecutor,
				destExecutor,
				loads,
				now,
			)
			if !found {
				continue
			}

			if err := moveShard(currentAssignments, sourceExecutor, destExecutor, shardToMove, idx); err != nil {
				return false, err
			}

			shardsMoved = true
			movesPlanned++
			moveBudget--
			movedThisIteration = true
			p.updateExecutorLoadsAfterMove(namespaceState, sourceExecutor, destExecutor, loads, shardToMove)
			p.logLoadBalanceMove(namespaceState, sourceExecutor, destExecutor, shardToMove, loads, metricsScope)
			break
		}

		if !movedThisIteration {
			stopReason = loadBalanceStopReasonNoEligibleShard
			break
		}
	}

	if movesPlanned > 0 {
		if metricsScope != nil {
			metricsScope.AddCounter(metrics.ShardDistributorAssignLoopLoadBasedMoves, int64(movesPlanned))
		}
		p.logger.Info("GREEDY load balancing planned shard moves",
			tag.Dynamic("moves_planned", movesPlanned),
			tag.Dynamic("mean_load", meanLoad),
			tag.Dynamic("initial_source_executors", len(initialSources)),
			tag.Dynamic("initial_destination_executors", len(initialDestinations)),
			tag.Dynamic("stop_reason", stopReason),
		)
		return shardsMoved, nil
	}

	p.logLoadBalanceSkipped(stopReason, meanLoad, len(initialSources), len(initialDestinations))
	return false, nil
}

func computeExecutorLoads(currentAssignments map[string][]string, namespaceState *store.NamespaceState) (map[string]float64, float64) {
	loads := make(map[string]float64, len(currentAssignments))
	total := 0.0

	for executorID, shards := range currentAssignments {
		loads[executorID] = 0
		for _, shardID := range shards {
			stats, ok := namespaceState.ShardStats[shardID]
			if !ok {
				continue
			}
			loads[executorID] += stats.SmoothedLoad
			total += stats.SmoothedLoad
		}
	}

	return loads, total
}

func activeExecutorSet(currentAssignments map[string][]string, namespaceState *store.NamespaceState) map[string]struct{} {
	activeExecutors := make(map[string]struct{})
	for executorID := range currentAssignments {
		if namespaceState.Executors[executorID].Status == types.ExecutorStatusACTIVE {
			activeExecutors[executorID] = struct{}{}
		}
	}
	return activeExecutors
}

func isSevereImbalance(executorLoads map[string]float64, meanLoad, severeImbalanceRatio float64) bool {
	if meanLoad <= 0 || severeImbalanceRatio <= 0 {
		return false
	}

	maxLoad := 0.0
	for _, load := range executorLoads {
		if load > maxLoad {
			maxLoad = load
		}
	}
	return maxLoad/meanLoad >= severeImbalanceRatio
}

func classifySourcesAndDestinations(
	executorLoads map[string]float64,
	namespaceState *store.NamespaceState,
	meanLoad float64,
	upperBand float64,
	lowerBand float64,
) (map[string]struct{}, map[string]struct{}) {
	sources := make(map[string]struct{})
	destinations := make(map[string]struct{})

	for executorID, load := range executorLoads {
		executor := namespaceState.Executors[executorID]
		if load > meanLoad*upperBand {
			sources[executorID] = struct{}{}
		} else if executor.Status == types.ExecutorStatusACTIVE && load < meanLoad*lowerBand {
			destinations[executorID] = struct{}{}
		}
	}

	return sources, destinations
}

func sourcesSortedByDescendingLoad(sourceExecutors map[string]struct{}, executorLoads map[string]float64) []string {
	sources := make([]string, 0, len(sourceExecutors))
	for executorID := range sourceExecutors {
		sources = append(sources, executorID)
	}

	slices.SortFunc(sources, func(a, b string) int {
		la, lb := executorLoads[a], executorLoads[b]
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

func findBestDestination(destinationExecutors map[string]struct{}, executorLoads map[string]float64) string {
	minLoad := math.MaxFloat64
	minExecutor := ""
	for executor := range destinationExecutors {
		load := executorLoads[executor]
		if load < minLoad {
			minLoad = load
			minExecutor = executor
		}
	}
	return minExecutor
}

func (p *namespaceProcessor) findShardToMove(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	executorLoads map[string]float64,
	now time.Time,
) (string, int, bool) {
	bestShard := ""
	perShardCooldown := p.cfg.LoadBalance.PerShardCooldown

	sourceLoad := executorLoads[source]
	destLoad := executorLoads[destination]
	idx := -1

	bestBenefit := 0.0
	for i, shard := range currentAssignments[source] {
		stats, ok := namespaceState.ShardStats[shard]
		if !ok || isShardInMoveCooldown(stats, perShardCooldown, now) {
			continue
		}

		benefit := computeBenefitOfMove(sourceLoad, destLoad, stats.SmoothedLoad)
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

func isShardInMoveCooldown(stats store.ShardStatistics, perShardCooldown time.Duration, now time.Time) bool {
	return perShardCooldown > 0 && !stats.LastMoveTime.IsZero() && now.Sub(stats.LastMoveTime) < perShardCooldown
}

func computeBenefitOfMove(sourceLoad, destLoad, shardLoad float64) float64 {
	return 2*shardLoad*(sourceLoad-destLoad) - 2*shardLoad*shardLoad
}

func moveShard(currentAssignments map[string][]string, sourceExecutor string, destExecutor string, shardID string, idx int) error {
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

func (p *namespaceProcessor) logLoadBalanceMove(
	namespaceState *store.NamespaceState,
	sourceExecutor string,
	destExecutor string,
	shardID string,
	executorLoads map[string]float64,
	metricsScope metrics.Scope,
) {
	stats := namespaceState.ShardStats[shardID]
	if metricsScope != nil {
		metricsScope.UpdateGauge(metrics.ShardDistributorAssignLoopMovedShardLoad, stats.SmoothedLoad)
	}
	p.logger.Info("GREEDY load-based shard move",
		tag.ShardKey(shardID),
		tag.ShardExecutor(sourceExecutor),
		tag.Dynamic("destination_executor", destExecutor),
		tag.Dynamic("shard_load", stats.SmoothedLoad),
		tag.Dynamic("source_load_after_move", executorLoads[sourceExecutor]),
		tag.Dynamic("destination_load_after_move", executorLoads[destExecutor]),
	)
}

func (p *namespaceProcessor) logLoadBalanceSkipped(stopReason string, meanLoad float64, sourceCount int, destinationCount int) {
	p.logger.Debug("GREEDY load balancing skipped",
		tag.Dynamic("reason", stopReason),
		tag.Dynamic("mean_load", meanLoad),
		tag.Dynamic("source_executors", sourceCount),
		tag.Dynamic("destination_executors", destinationCount),
	)
}

func (p *namespaceProcessor) emitAssignmentImbalanceMetrics(
	metricsScope metrics.Scope,
	assignments map[string][]string,
	namespaceState *store.NamespaceState,
) {
	if metricsScope == nil || namespaceState == nil {
		return
	}

	now := p.timeSource.Now().UTC()
	staleAfter := p.cfg.HeartbeatTTL

	reportedLoads := make([]float64, 0, len(assignments))
	smoothedLoads := make([]float64, 0, len(assignments))

	totalAssigned := 0
	reportedMissing := 0
	smoothedMissing := 0
	smoothedStale := 0

	for executorID, shards := range assignments {
		reportedLoad := 0.0
		smoothedLoad := 0.0

		heartbeat, heartbeatOK := namespaceState.Executors[executorID]
		for _, shardID := range shards {
			totalAssigned++

			if !heartbeatOK || heartbeat.ReportedShards == nil {
				reportedMissing++
			} else if shardReport, ok := heartbeat.ReportedShards[shardID]; ok && shardReport != nil {
				reportedLoad += shardReport.ShardLoad
			} else {
				reportedMissing++
			}

			if namespaceState.ShardStats == nil {
				smoothedMissing++
				continue
			}
			stats, ok := namespaceState.ShardStats[shardID]
			if !ok || stats.LastUpdateTime.IsZero() {
				smoothedMissing++
				continue
			}
			if staleAfter > 0 && now.Sub(stats.LastUpdateTime) > staleAfter {
				smoothedStale++
			}
			smoothedLoad += stats.SmoothedLoad
		}

		reportedLoads = append(reportedLoads, reportedLoad)
		smoothedLoads = append(smoothedLoads, smoothedLoad)
	}

	metricsScope.UpdateGauge(metrics.ShardDistributorAssignmentLoadMaxOverMean, maxOverMean(reportedLoads))
	metricsScope.UpdateGauge(metrics.ShardDistributorAssignmentLoadCV, coefficientOfVariation(reportedLoads))
	metricsScope.UpdateGauge(metrics.ShardDistributorAssignmentSmoothedLoadMaxOverMean, maxOverMean(smoothedLoads))
	metricsScope.UpdateGauge(metrics.ShardDistributorAssignmentSmoothedLoadCV, coefficientOfVariation(smoothedLoads))

	if totalAssigned == 0 {
		metricsScope.UpdateGauge(metrics.ShardDistributorAssignmentReportedLoadMissingRatio, 0)
		metricsScope.UpdateGauge(metrics.ShardDistributorAssignmentSmoothedLoadMissingRatio, 0)
		metricsScope.UpdateGauge(metrics.ShardDistributorAssignmentSmoothedLoadStaleRatio, 0)
		return
	}

	metricsScope.UpdateGauge(metrics.ShardDistributorAssignmentReportedLoadMissingRatio, float64(reportedMissing)/float64(totalAssigned))
	metricsScope.UpdateGauge(metrics.ShardDistributorAssignmentSmoothedLoadMissingRatio, float64(smoothedMissing)/float64(totalAssigned))
	metricsScope.UpdateGauge(metrics.ShardDistributorAssignmentSmoothedLoadStaleRatio, float64(smoothedStale)/float64(totalAssigned))
}

func maxOverMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	total := 0.0
	maxValue := 0.0
	for _, value := range values {
		total += value
		if value > maxValue {
			maxValue = value
		}
	}

	mean := total / float64(len(values))
	if mean == 0 {
		return 0
	}
	return maxValue / mean
}

func coefficientOfVariation(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	total := 0.0
	for _, value := range values {
		total += value
	}
	mean := total / float64(len(values))
	if mean == 0 {
		return 0
	}

	variance := 0.0
	for _, value := range values {
		delta := value - mean
		variance += delta * delta
	}
	variance /= float64(len(values))

	return math.Sqrt(variance) / mean
}
