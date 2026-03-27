package process

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"time"

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
		return false, nil
	}

	meanLoad := totalLoad / float64(len(loads))
	allShards := getShards(p.namespaceCfg, namespaceState, deletedShards)
	moveBudget := computeMoveBudget(len(allShards), p.cfg.LoadBalance.MoveBudgetProportion)
	shardsMoved := false
	movesPlanned := 0
	now := p.timeSource.Now().UTC()

	if moveBudget <= 0 {
		return false, nil
	}

	// Plan multiple moves per cycle (within budget), recomputing eligibility after each move.
	// Stop early once sources/destinations are empty, i.e. imbalance is within hysteresis bands.
	for moveBudget > 0 {
		sourceExecutors, destinationExecutors := classifySourcesAndDestinations(
			loads,
			namespaceState,
			meanLoad,
			p.cfg.LoadBalance.HysteresisUpperBand,
			p.cfg.LoadBalance.HysteresisLowerBand,
		)

		if len(sourceExecutors) == 0 {
			break
		}

		// Escape hatch: if we have sources but no destinations under the normal lower band,
		// allow moving to the least-loaded ACTIVE executor when imbalance is severe.
		if len(destinationExecutors) == 0 {
			if !isSevereImbalance(loads, meanLoad, p.cfg.LoadBalance.SevereImbalanceRatio) {
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

		sources := sourcesSortedByDescendingLoad(sourceExecutors, loads)

		destExecutor := p.findBestDestination(destinationExecutors, loads)
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
			moves, found := p.findShardsToMove(
				currentAssignments,
				namespaceState,
				sourceExecutor,
				destExecutor,
				loads,
				now,
			)
			if !found {
				// No eligible shard for this source+destination (cooldown, or no beneficial move), try the next source.
				continue
			}

			if err := p.moveShards(currentAssignments, moves); err != nil {
				return false, err
			}
			movesPlanned++
			shardsMoved = true

			p.updateExecutorLoadsAfterMove(namespaceState, loads, moves)
			moveBudget--
			movedThisIteration = true
			break
		}

		// No eligible shard could be moved from any source.
		if !movedThisIteration {
			break
		}
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

// classifySourcesAndDestinations returns the source and destination executor sets for rebalancing.
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

// sourcesSortedByDescendingLoad orders sources by descending load so we prefer to
// move shards away from the hottest executors first. Exact ordering among equal loads is not important.
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

func (p *namespaceProcessor) findBestDestination(destinationExecutors map[string]struct{}, executorLoads map[string]float64) string {
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

// findShardToMove returns the best shard to move from source to destination.
func (p *namespaceProcessor) findShardsToMove(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	executorLoads map[string]float64,
	now time.Time,
) ([]ShardMove, bool) {
	perShardCooldown := p.cfg.LoadBalance.PerShardCooldown
	sourceLoad := executorLoads[source]
	destLoad := executorLoads[destination]

	var singleMove []ShardMove
	var singleMoveBenefit float64
	singleMove, _, singleMoveBenefit = p.findSingleShard(
		currentAssignments,
		namespaceState,
		source,
		destination,
		sourceLoad,
		destLoad,
		perShardCooldown,
		now,
	)

	var swapMoves []ShardMove
	var swapMoveBenefit float64
	swapMoves, _, swapMoveBenefit = findSwapShards(
		currentAssignments,
		namespaceState,
		source,
		destination,
		sourceLoad,
		destLoad,
		perShardCooldown,
		now,
	)

	if singleMoveBenefit == 0 && swapMoveBenefit == 0 {
		return nil, false
	}

	if singleMoveBenefit >= swapMoveBenefit {
		return singleMove, true
	} else {
		return swapMoves, true
	}
}

// computeBenefitOfMove returns the expected reduction in sum of squared error (SSE)
// around the mean load if we move a shard with shardLoad from sourceLoad to destLoad.
// A positive value means the move improves overall load balance.
func computeBenefitOfMove(sourceLoad, destLoad, shardLoad float64) float64 {
	w := shardLoad
	return 2*w*(sourceLoad-destLoad) - 2*w*w
}

type ShardMove struct {
	shardID     string
	source      string
	destination string
}

type shardInfo struct {
	id   string
	load float64
}

func (p *namespaceProcessor) findSingleShard(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	sourceLoad float64,
	destLoad float64,
	perShardCooldown time.Duration,
	now time.Time,
) ([]ShardMove, bool, float64) {
	bestShard := ""
	bestSingleMoveBenefit := 0.0
	for _, shard := range currentAssignments[source] {
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
		if benefit > bestSingleMoveBenefit {
			bestSingleMoveBenefit = benefit
			bestShard = shard
		}
	}

	return []ShardMove{{shardID: bestShard, source: source, destination: destination}}, bestShard != "", bestSingleMoveBenefit
}

func findSwapShards(
	currentAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	source string,
	destination string,
	sourceLoad float64,
	destLoad float64,
	perShardCooldown time.Duration,
	now time.Time,
) ([]ShardMove, bool, float64) {
	var eligibleShardsSource []shardInfo
	for _, shardID := range currentAssignments[source] {
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
	var bestMoves []ShardMove
	found := false
	for _, dShard := range eligibleShardsDestination {
		idx := -1
		searchTarget := idealLoad + dShard.load

		idx, _ = slices.BinarySearchFunc(eligibleShardsSource, searchTarget, func(s shardInfo, target float64) int {
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
				bestMoves = []ShardMove{
					{shardID: sShard.id, source: source, destination: destination},
					{shardID: dShard.id, source: destination, destination: source},
				}
				found = true
			}
		}
	}
	if found {
		return bestMoves, true, computeBenefitOfMove(sourceLoad, destLoad, bestActualMove)
	}
	return nil, false, 0
}

func (p *namespaceProcessor) moveShards(currentAssignments map[string][]string, moves []ShardMove) error {
	for _, move := range moves {
		idx := slices.IndexFunc(currentAssignments[move.source], func(c string) bool { return c == move.shardID })
		// defensive fallback in case index is stale
		if idx < 0 || idx >= len(currentAssignments[move.source]) || currentAssignments[move.source][idx] != move.shardID {
			idx = slices.Index(currentAssignments[move.source], move.shardID)
		}
		//
		if idx == -1 {
			return fmt.Errorf("shard %s not found in source executor %s", move.shardID, move.source)
		}

		// Remove shard from source.
		currentAssignments[move.source][idx] = currentAssignments[move.source][len(currentAssignments[move.source])-1]
		currentAssignments[move.source] = currentAssignments[move.source][:len(currentAssignments[move.source])-1]

		// Add shard to destination.
		currentAssignments[move.destination] = append(currentAssignments[move.destination], move.shardID)
	}
	return nil
}

func (p *namespaceProcessor) updateExecutorLoadsAfterMove(
	namespaceState *store.NamespaceState,
	executorLoads map[string]float64,
	moves []ShardMove,
) {
	for _, move := range moves {
		stats, ok := namespaceState.ShardStats[move.shardID]
		if !ok {
			continue
		}
		executorLoads[move.source] -= stats.SmoothedLoad
		executorLoads[move.destination] += stats.SmoothedLoad
	}
}
