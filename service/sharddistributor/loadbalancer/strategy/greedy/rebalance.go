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
	"github.com/uber/cadence/service/sharddistributor/config"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/plan"
	"github.com/uber/cadence/service/sharddistributor/store"
)

// PlanRebalance returns planned shard moves for the current assignment state.
func PlanRebalance(
	cfg config.LoadBalancingGreedyConfig,
	namespace string,
	namespaceState *store.NamespaceState,
	currentAssignments map[string][]string,
	now time.Time,
	logger log.Logger,
	metricsScope metrics.Scope,
) ([]plan.Move, error) {
	now = now.UTC()
	workingAssignments := cloneAssignments(currentAssignments)
	loads, totalLoad := computeExecutorLoads(workingAssignments, namespaceState)
	if len(loads) == 0 {
		return nil, nil
	}

	meanLoad := totalLoad / float64(len(loads))
	totalShards := 0
	for _, shards := range currentAssignments {
		totalShards += len(shards)
	}
	moveBudget := computeMoveBudget(totalShards, cfg.MoveBudgetProportion(namespace))
	if moveBudget <= 0 {
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
			meanLoad,
			cfg.HysteresisUpperBand(namespace),
			cfg.HysteresisLowerBand(namespace),
		)

		if len(sourceExecutors) == 0 {
			break
		}

		// If we have sources but no destinations under the normal lower band,
		// allow moving to the least-loaded ACTIVE executor when imbalance is severe.
		if len(destinationExecutors) == 0 {
			if !isSevereImbalance(loads, meanLoad, cfg.SevereImbalanceRatio(namespace)) {
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

		sources := sourcesSortedByDescendingLoad(sourceExecutors, loads)

		destExecutor := findBestDestination(destinationExecutors, loads)
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
			shardsToMove, found, moveType, bestSingle := findShardsToMove(
				workingAssignments,
				namespaceState,
				sourceExecutor,
				destExecutor,
				loads,
				movedShards,
				now,
				cfg.PerShardCooldown(namespace),
				cfg.EnableSwap,
			)
			if !found {
				// No eligible shard for this source+destination (cooldown, or no beneficial move), try the next source.
				continue
			}

			if logger != nil {
				logger.Info("load balance move planned",
					tag.Dynamic("move_type", moveType),
					tag.Dynamic("source_executor", sourceExecutor),
					tag.Dynamic("source_load_before", loads[sourceExecutor]),
					tag.Dynamic("source_shards_before", shardListWithLoads(workingAssignments[sourceExecutor], namespaceState)),
					tag.Dynamic("destination_executor", destExecutor),
					tag.Dynamic("destination_load_before", loads[destExecutor]),
					tag.Dynamic("destination_shards_before", shardListWithLoads(workingAssignments[destExecutor], namespaceState)),
					tag.Dynamic("all_executors", allExecutorsState(workingAssignments, loads, namespaceState)),
					tag.Dynamic("planned_moves", shardsToMove),
				)
				if moveType == "swap" && len(bestSingle) > 0 {
					logger.Info("load balance rejected single move",
						tag.Dynamic("source_executor", sourceExecutor),
						tag.Dynamic("destination_executor", destExecutor),
						tag.Dynamic("best_single_move", bestSingle),
					)
				}
			}

			if err := moveShards(workingAssignments, shardsToMove); err != nil {
				return nil, err
			}
			moves = append(moves, shardsToMove...)
			for _, m := range shardsToMove {
				movedShards[m.ShardID] = struct{}{}
				if metricsScope != nil {
					metricsScope.UpdateGauge(metrics.ShardDistributorAssignLoopMovedShardLoad, namespaceState.ShardStats[m.ShardID].SmoothedLoad)
				}
			}
			updateExecutorLoadsAfterMoves(namespaceState, loads, shardsToMove)

			if logger != nil {
				logger.Info("load balance move applied",
					tag.Dynamic("source_executor", sourceExecutor),
					tag.Dynamic("source_load_after", loads[sourceExecutor]),
					tag.Dynamic("source_shards_after", shardListWithLoads(workingAssignments[sourceExecutor], namespaceState)),
					tag.Dynamic("destination_executor", destExecutor),
					tag.Dynamic("destination_load_after", loads[destExecutor]),
					tag.Dynamic("destination_shards_after", shardListWithLoads(workingAssignments[destExecutor], namespaceState)),
					tag.Dynamic("all_executors", allExecutorsState(workingAssignments, loads, namespaceState)),
				)
			}

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

// shardListWithLoads returns a slice of "shardID:load" strings for logging.
func shardListWithLoads(shardIDs []string, state *store.NamespaceState) []string {
	result := make([]string, 0, len(shardIDs))
	for _, id := range shardIDs {
		load := 0.0
		if stats, ok := state.ShardStats[id]; ok {
			load = stats.SmoothedLoad
		}
		result = append(result, fmt.Sprintf("%s:%.2f", id, load))
	}
	return result
}

// allExecutorsState returns a map of every executor to its current load and shard list.
func allExecutorsState(
	assignments map[string][]string,
	loads map[string]float64,
	state *store.NamespaceState,
) map[string]interface{} {
	result := make(map[string]interface{}, len(assignments))
	for executorID, shards := range assignments {
		result[executorID] = map[string]interface{}{
			"load":   loads[executorID],
			"shards": shardListWithLoads(shards, state),
		}
	}
	return result
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
			stats, ok := state.ShardStats[shardID]
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

func computeMoveBudget(totalShards int, proportion float64) int {
	if totalShards <= 0 || proportion <= 0 {
		return 0
	}
	return int(math.Ceil(proportion * float64(totalShards)))
}

func classifySourcesAndDestinations(
	executorLoads map[string]float64,
	state *store.NamespaceState,
	meanLoad float64,
	upperBand float64,
	lowerBand float64,
) (map[string]struct{}, map[string]struct{}) {
	sources := make(map[string]struct{})
	destinations := make(map[string]struct{})

	for executorID, load := range executorLoads {
		executor := state.Executors[executorID]
		// Intentionally allow DRAINING executors as sources so they can shed shards
		if load > meanLoad*upperBand {
			sources[executorID] = struct{}{}
		} else if executor.Status == types.ExecutorStatusACTIVE && load < meanLoad*lowerBand {
			destinations[executorID] = struct{}{}
		}
	}

	return sources, destinations
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

type shardInfo struct {
	id   string
	load float64
}

// findShardsToMove evaluates single moves and pairwise swaps, returning the best option.
// When a swap wins, bestSingle contains the best single move that was rejected.
func findShardsToMove(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	executorLoads map[string]float64,
	movedShards map[string]struct{},
	now time.Time,
	perShardCooldown time.Duration,
	enableSwap bool,
) (moves []plan.Move, found bool, moveType string, bestSingle []plan.Move) {
	sourceLoad := executorLoads[source]
	destLoad := executorLoads[destination]

	singleMove, singleMoveBenefit := findSingleShard(
		currentAssignments,
		namespaceState,
		source,
		destination,
		sourceLoad,
		destLoad,
		movedShards,
		perShardCooldown,
		now,
	)

	if !enableSwap {
		if singleMoveBenefit <= 0 {
			return nil, false, "", nil
		}
		return singleMove, true, "single", nil
	}

	swapMoves, swapMoveBenefit := findSwapShards(
		currentAssignments,
		namespaceState,
		source,
		destination,
		sourceLoad,
		destLoad,
		movedShards,
		perShardCooldown,
		now,
	)

	if singleMoveBenefit <= 0 && swapMoveBenefit <= 0 {
		return nil, false, "", nil
	}

	if singleMoveBenefit >= swapMoveBenefit {
		return singleMove, true, "single", nil
	}
	return swapMoves, true, "swap", singleMove
}

// computeBenefitOfMove returns the expected reduction in sum of squared error (SSE)
// around the mean load if we move a shard with shardLoad from sourceLoad to destLoad.
// A positive value means the move improves overall load balance.
func computeBenefitOfMove(sourceLoad, destLoad, shardLoad float64) float64 {
	w := shardLoad
	return 2*w*(sourceLoad-destLoad) - 2*w*w
}

func findSingleShard(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	sourceLoad float64,
	destLoad float64,
	movedShards map[string]struct{},
	perShardCooldown time.Duration,
	now time.Time,
) ([]plan.Move, float64) {
	bestShard := ""
	bestBenefit := 0.0

	for _, shard := range currentAssignments[source] {
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
		benefit := computeBenefitOfMove(sourceLoad, destLoad, load)
		if benefit <= 0 {
			continue
		}
		if benefit > bestBenefit {
			bestBenefit = benefit
			bestShard = shard
		}
	}

	if bestShard == "" {
		return nil, 0
	}
	return []plan.Move{{ShardID: bestShard, From: source, To: destination}}, bestBenefit
}

func findSwapShards(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	sourceLoad float64,
	destLoad float64,
	movedShards map[string]struct{},
	perShardCooldown time.Duration,
	now time.Time,
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

	idealLoad := (sourceLoad - destLoad) / 2
	bestDiff := idealLoad
	bestActualMove := 0.0
	var bestMoves []plan.Move
	found := false

	for _, dShard := range eligibleShardsDestination {
		searchTarget := idealLoad + dShard.load

		idx, _ := slices.BinarySearchFunc(eligibleShardsSource, searchTarget, func(s shardInfo, target float64) int {
			return cmp.Compare(target, s.load)
		})

		for _, i := range []int{idx - 1, idx, idx + 1} {
			if i < 0 || i >= len(eligibleShardsSource) {
				continue
			}
			sShard := eligibleShardsSource[i]

			actualMove := sShard.load - dShard.load
			diff := math.Abs(idealLoad - actualMove)

			if diff < bestDiff {
				bestDiff = diff
				bestActualMove = actualMove
				bestMoves = []plan.Move{
					{ShardID: sShard.id, From: source, To: destination},
					{ShardID: dShard.id, From: destination, To: source},
				}
				found = true
			}
		}
	}

	if !found {
		return nil, 0
	}
	return bestMoves, computeBenefitOfMove(sourceLoad, destLoad, bestActualMove)
}

func moveShards(currentAssignments map[string][]string, moves []plan.Move) error {
	for _, move := range moves {
		idx := slices.IndexFunc(currentAssignments[move.From], func(c string) bool { return c == move.ShardID })
		// Defensive fallback in case index is stale
		if idx < 0 || idx >= len(currentAssignments[move.From]) || currentAssignments[move.From][idx] != move.ShardID {
			idx = slices.Index(currentAssignments[move.From], move.ShardID)
		}
		if idx == -1 {
			return fmt.Errorf("shard %s not found in source executor %s", move.ShardID, move.From)
		}

		// Remove shard from source
		currentAssignments[move.From][idx] = currentAssignments[move.From][len(currentAssignments[move.From])-1]
		currentAssignments[move.From] = currentAssignments[move.From][:len(currentAssignments[move.From])-1]

		// Add shard to destination
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
		stats, ok := state.ShardStats[move.ShardID]
		if !ok {
			continue
		}
		executorLoads[move.From] -= stats.SmoothedLoad
		executorLoads[move.To] += stats.SmoothedLoad
	}
}
