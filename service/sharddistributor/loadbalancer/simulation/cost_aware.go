package simulation

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/plan"
)

type costAwareMoveCandidate struct {
	move    plan.Move
	idx     int
	benefit float64
	score   float64
}

func (s *Simulator) planCostAwareMoves() ([]plan.Move, error) {
	workingAssignments := cloneSimulationAssignments(s.assignments)
	loads := s.SmoothedExecutorLoads()
	if len(loads) == 0 {
		return nil, nil
	}

	meanLoad := mean(loads)
	if meanLoad == 0 {
		return nil, nil
	}

	moveBudget := s.costAwareMoveBudget(loads)
	if moveBudget <= 0 {
		return nil, nil
	}

	moves := make([]plan.Move, 0, moveBudget)
	movedShards := make(map[string]struct{}, moveBudget)
	sourceMoves := make(map[string]int, len(loads))
	destinationMoves := make(map[string]int, len(loads))

	for remainingBudget := moveBudget; remainingBudget > 0; remainingBudget-- {
		candidate, ok := s.bestCostAwareCandidate(
			workingAssignments,
			loads,
			meanLoad,
			movedShards,
			sourceMoves,
			destinationMoves,
		)
		if !ok {
			break
		}

		if err := moveSimulationShard(workingAssignments, candidate.move.From, candidate.move.To, candidate.move.ShardID, candidate.idx); err != nil {
			return nil, err
		}
		updateExecutorLoadsAfterSimulationMove(s.state.ShardStats[candidate.move.ShardID].SmoothedLoad, candidate.move.From, candidate.move.To, loads)
		movedShards[candidate.move.ShardID] = struct{}{}
		sourceMoves[candidate.move.From]++
		destinationMoves[candidate.move.To]++
		moves = append(moves, candidate.move)
	}

	return moves, nil
}

func (s *Simulator) costAwareMoveBudget(loads map[string]float64) int {
	totalShards := s.shardCount()
	baseBudget := computeSimulationMoveBudget(totalShards, s.cfg.MoveBudgetProportion)
	if baseBudget <= 0 {
		return 0
	}

	cv := coefficientOfVariation(loads)
	if cv < s.cfg.CostAwareTargetCV {
		return 0
	}
	if cv < s.cfg.CostAwareTargetCV*1.5 {
		return min(baseBudget, 1)
	}
	if maxOverMean(loads) < s.cfg.SevereImbalanceRatio {
		return min(baseBudget, 2)
	}
	return baseBudget
}

func (s *Simulator) bestCostAwareCandidate(
	assignments map[string][]string,
	loads map[string]float64,
	meanLoad float64,
	movedShards map[string]struct{},
	sourceMoves map[string]int,
	destinationMoves map[string]int,
) (costAwareMoveCandidate, bool) {
	var best costAwareMoveCandidate
	found := false
	totalSSE := executorSSE(loads, meanLoad)
	minBenefit := totalSSE * s.cfg.CostAwareMinBenefitRatio
	moveCost := totalSSE * s.cfg.CostAwareMoveCostRatio
	severe := maxOverMean(loads) >= s.cfg.SevereImbalanceRatio
	if minBenefit < meanLoad*0.01 {
		minBenefit = meanLoad * 0.01
	}

	for source, shards := range assignments {
		if loads[source] <= meanLoad*s.cfg.HysteresisUpperBand {
			continue
		}
		if !severe && sourceMoves[source] >= 1 {
			continue
		}
		for destination := range assignments {
			if source == destination {
				continue
			}
			if !severe && destinationMoves[destination] >= 1 {
				continue
			}
			if s.state.Executors[destination].Status != types.ExecutorStatusACTIVE {
				continue
			}
			if !s.costAwareDestinationEligible(loads, destination, meanLoad) {
				continue
			}
			for idx, shardID := range shards {
				if _, ok := movedShards[shardID]; ok {
					continue
				}
				stats, ok := s.state.ShardStats[shardID]
				if !ok || !s.shardMoveCooldownElapsed(stats.LastMoveTime) {
					continue
				}

				load := stats.SmoothedLoad
				if load <= 0 {
					continue
				}
				benefit := computeSimulationMoveBenefit(loads[source], loads[destination], load)
				if benefit <= minBenefit {
					continue
				}
				projectedSource := loads[source] - load
				projectedDestination := loads[destination] + load
				if projectedDestination > projectedSource && projectedDestination > meanLoad*s.cfg.HysteresisUpperBand {
					continue
				}

				score := benefit - moveCost - overshootPenalty(projectedSource, projectedDestination, meanLoad)
				if !found || score > best.score || (score == best.score && tieBreakMove(source, destination, shardID, best.move)) {
					best = costAwareMoveCandidate{
						move: plan.Move{
							ShardID: shardID,
							From:    source,
							To:      destination,
						},
						idx:     idx,
						benefit: benefit,
						score:   score,
					}
					found = true
				}
			}
		}
	}

	return best, found
}

func (s *Simulator) costAwareDestinationEligible(loads map[string]float64, destination string, meanLoad float64) bool {
	if loads[destination] < meanLoad*s.cfg.HysteresisLowerBand {
		return true
	}
	return maxOverMean(loads) >= s.cfg.SevereImbalanceRatio && loads[destination] < meanLoad
}

func (s *Simulator) shardMoveCooldownElapsed(lastMove time.Time) bool {
	return s.cfg.PerShardCooldown <= 0 || lastMove.IsZero() || s.now.Sub(lastMove) >= s.cfg.PerShardCooldown
}

func updateExecutorLoadsAfterSimulationMove(shardLoad float64, source string, destination string, loads map[string]float64) {
	loads[source] -= shardLoad
	loads[destination] += shardLoad
}

func cloneSimulationAssignments(assignments map[string][]string) map[string][]string {
	cloned := make(map[string][]string, len(assignments))
	for executorID, shardIDs := range assignments {
		clonedShards := make([]string, len(shardIDs))
		copy(clonedShards, shardIDs)
		cloned[executorID] = clonedShards
	}
	return cloned
}

func computeSimulationMoveBenefit(sourceLoad float64, destinationLoad float64, shardLoad float64) float64 {
	return 2*shardLoad*(sourceLoad-destinationLoad) - 2*shardLoad*shardLoad
}

func computeSimulationMoveBudget(totalShards int, proportion float64) int {
	if totalShards <= 0 || proportion <= 0 {
		return 0
	}
	return int(math.Ceil(proportion * float64(totalShards)))
}

func executorSSE(loads map[string]float64, meanLoad float64) float64 {
	sse := 0.0
	for _, load := range loads {
		delta := load - meanLoad
		sse += delta * delta
	}
	return sse
}

func mean(loads map[string]float64) float64 {
	if len(loads) == 0 {
		return 0
	}
	total := 0.0
	for _, load := range loads {
		total += load
	}
	return total / float64(len(loads))
}

func overshootPenalty(projectedSource float64, projectedDestination float64, meanLoad float64) float64 {
	sourceDeviation := math.Abs(projectedSource - meanLoad)
	destinationDeviation := math.Abs(projectedDestination - meanLoad)
	if projectedDestination <= meanLoad || projectedDestination <= projectedSource {
		return 0
	}
	return sourceDeviation + destinationDeviation
}

func tieBreakMove(source string, destination string, shardID string, existing plan.Move) bool {
	return slices.Compare([]string{source, destination, shardID}, []string{existing.From, existing.To, existing.ShardID}) < 0
}

func moveSimulationShard(currentAssignments map[string][]string, sourceExecutor string, destExecutor string, shardID string, idx int) error {
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
