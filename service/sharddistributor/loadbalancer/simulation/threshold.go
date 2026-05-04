package simulation

import (
	"math"

	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/plan"
)

func (s *Simulator) planThresholdMoves() ([]plan.Move, error) {
	workingAssignments := cloneSimulationAssignments(s.assignments)
	loads := s.SmoothedExecutorLoads()
	meanLoad := mean(loads)
	if meanLoad == 0 {
		return nil, nil
	}

	moveBudget := computeSimulationMoveBudget(s.shardCount(), s.cfg.MoveBudgetProportion)
	if moveBudget <= 0 {
		return nil, nil
	}

	moves := make([]plan.Move, 0, moveBudget)
	movedShards := make(map[string]struct{}, moveBudget)
	for len(moves) < moveBudget {
		source := s.thresholdSource(loads, meanLoad)
		if source == "" {
			break
		}
		destination := s.thresholdDestination(loads, meanLoad)
		if destination == "" || destination == source {
			break
		}

		shardID, idx, ok := s.thresholdShard(workingAssignments[source], loads[source], loads[destination], movedShards)
		if !ok {
			break
		}
		if err := moveSimulationShard(workingAssignments, source, destination, shardID, idx); err != nil {
			return nil, err
		}
		shardLoad := s.state.ShardStats[shardID].SmoothedLoad
		updateExecutorLoadsAfterSimulationMove(shardLoad, source, destination, loads)
		movedShards[shardID] = struct{}{}
		moves = append(moves, plan.Move{
			ShardID: shardID,
			From:    source,
			To:      destination,
		})
	}

	return moves, nil
}

func (s *Simulator) thresholdSource(loads map[string]float64, meanLoad float64) string {
	source := ""
	sourceLoad := meanLoad * s.cfg.HysteresisUpperBand
	for executorID, load := range loads {
		if load > sourceLoad {
			source = executorID
			sourceLoad = load
		}
	}
	return source
}

func (s *Simulator) thresholdDestination(loads map[string]float64, meanLoad float64) string {
	destination := ""
	destinationLoad := math.MaxFloat64
	severe := maxOverMean(loads) >= s.cfg.SevereImbalanceRatio
	for executorID, load := range loads {
		if s.state.Executors[executorID].Status != types.ExecutorStatusACTIVE {
			continue
		}
		if load >= meanLoad*s.cfg.HysteresisLowerBand && !(severe && load < meanLoad) {
			continue
		}
		if load < destinationLoad {
			destination = executorID
			destinationLoad = load
		}
	}
	return destination
}

func (s *Simulator) thresholdShard(
	shards []string,
	sourceLoad float64,
	destinationLoad float64,
	movedShards map[string]struct{},
) (string, int, bool) {
	bestShard := ""
	bestIdx := -1
	bestDistance := math.MaxFloat64
	targetLoad := (sourceLoad - destinationLoad) / 2
	for idx, shardID := range shards {
		if _, ok := movedShards[shardID]; ok {
			continue
		}
		stats, ok := s.state.ShardStats[shardID]
		if !ok || !s.shardMoveCooldownElapsed(stats.LastMoveTime) {
			continue
		}
		load := stats.SmoothedLoad
		if load <= 0 || computeSimulationMoveBenefit(sourceLoad, destinationLoad, load) <= 0 {
			continue
		}
		distance := math.Abs(load - targetLoad)
		if distance < bestDistance {
			bestShard = shardID
			bestIdx = idx
			bestDistance = distance
		}
	}
	return bestShard, bestIdx, bestShard != ""
}
