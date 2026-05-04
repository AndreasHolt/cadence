package simulation

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/config"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/plan"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/strategy/greedy"
	"github.com/uber/cadence/service/sharddistributor/statistics"
	"github.com/uber/cadence/service/sharddistributor/store"
)

// Config controls a deterministic in-memory replay run.
type Config struct {
	Namespace string

	ExecutorCount int

	PerShardCooldown     time.Duration
	MoveBudgetProportion float64
	HysteresisUpperBand  float64
	HysteresisLowerBand  float64
	SevereImbalanceRatio float64
}

// Result is the compact feedback payload for tuning load balancing behavior.
type Result struct {
	Rows          int            `json:"rows"`
	Shards        int            `json:"shards"`
	Executors     int            `json:"executors"`
	Moves         int            `json:"moves"`
	FinalCounts   map[string]int `json:"final_counts"`
	LastTimestamp time.Time      `json:"last_timestamp"`

	FinalSmoothedLoads         map[string]float64 `json:"final_smoothed_loads"`
	FinalSmoothedMaxOverMean   float64            `json:"final_smoothed_max_over_mean"`
	FinalSmoothedCV            float64            `json:"final_smoothed_cv"`
	AverageSmoothedMaxOverMean float64            `json:"average_smoothed_max_over_mean"`
	AverageSmoothedCV          float64            `json:"average_smoothed_cv"`
	WorstSmoothedMaxOverMean   float64            `json:"worst_smoothed_max_over_mean"`
	WorstSmoothedCV            float64            `json:"worst_smoothed_cv"`

	FinalReportedLoads         map[string]float64 `json:"final_reported_loads"`
	FinalReportedMaxOverMean   float64            `json:"final_reported_max_over_mean"`
	FinalReportedCV            float64            `json:"final_reported_cv"`
	AverageReportedMaxOverMean float64            `json:"average_reported_max_over_mean"`
	AverageReportedCV          float64            `json:"average_reported_cv"`
	WorstReportedMaxOverMean   float64            `json:"worst_reported_max_over_mean"`
	WorstReportedCV            float64            `json:"worst_reported_cv"`
}

// Simulator runs the refactored greedy planner against in-memory state.
type Simulator struct {
	cfg         Config
	greedyCfg   config.LoadBalancingGreedyConfig
	state       *store.NamespaceState
	assignments map[string][]string
	now         time.Time
	moves       int

	samples                  int
	smoothedCVTotal          float64
	smoothedMaxOverMeanTotal float64
	worstSmoothedCV          float64
	worstSmoothedMaxOverMean float64
	reportedCVTotal          float64
	reportedMaxOverMeanTotal float64
	worstReportedCV          float64
	worstReportedMaxOverMean float64
	finalReportedLoads       map[string]float64
}

func New(cfg Config, start time.Time, shardIDs []string) (*Simulator, error) {
	cfg = cfg.withDefaults()
	if cfg.ExecutorCount <= 0 {
		return nil, fmt.Errorf("executor count must be positive")
	}
	if len(shardIDs) == 0 {
		return nil, fmt.Errorf("shard IDs must not be empty")
	}

	s := &Simulator{
		cfg:       cfg,
		greedyCfg: greedyConfig(cfg),
		state: &store.NamespaceState{
			Executors:        make(map[string]store.HeartbeatState, cfg.ExecutorCount),
			ShardStats:       make(map[string]store.ShardStatistics, len(shardIDs)),
			ShardAssignments: make(map[string]store.AssignedState, cfg.ExecutorCount),
		},
		assignments: make(map[string][]string, cfg.ExecutorCount),
		now:         start.UTC(),
	}

	executors := make([]string, cfg.ExecutorCount)
	for i := range executors {
		executorID := fmt.Sprintf("exec-%d", i)
		executors[i] = executorID
		s.assignments[executorID] = nil
		s.state.Executors[executorID] = store.HeartbeatState{
			Status:        types.ExecutorStatusACTIVE,
			LastHeartbeat: s.now,
		}
		s.state.ShardAssignments[executorID] = store.AssignedState{
			AssignedShards: make(map[string]*types.ShardAssignment),
			LastUpdated:    s.now,
		}
	}

	for i, shardID := range shardIDs {
		executorID := executors[i%len(executors)]
		s.assignments[executorID] = append(s.assignments[executorID], shardID)
		s.state.ShardAssignments[executorID].AssignedShards[shardID] = &types.ShardAssignment{Status: types.AssignmentStatusREADY}
		s.state.ShardStats[shardID] = store.ShardStatistics{LastUpdateTime: time.Time{}}
	}

	return s, nil
}

// ApplyRow advances simulated time to the row timestamp, updates smoothed load,
// and runs one greedy rebalance pass.
func (s *Simulator) ApplyRow(row LoadHistoryRow) error {
	s.now = row.Timestamp.UTC()
	for shardID, load := range row.ShardLoads {
		stats := s.state.ShardStats[shardID]
		smoothedLoad, err := statistics.CalculateSmoothedLoad(stats.SmoothedLoad, load, stats.LastUpdateTime, s.now)
		if err != nil {
			return fmt.Errorf("calculate smoothed load for shard %s: %w", shardID, err)
		}
		stats.SmoothedLoad = smoothedLoad
		stats.LastUpdateTime = s.now
		s.state.ShardStats[shardID] = stats
	}

	moves, err := greedy.PlanRebalance(s.greedyCfg, s.cfg.Namespace, s.state, s.assignments, s.now, metrics.NoopScope)
	if err != nil {
		return fmt.Errorf("plan rebalance: %w", err)
	}
	if err := applyMoves(s.assignments, moves); err != nil {
		return fmt.Errorf("apply moves: %w", err)
	}
	s.moves += len(moves)
	for _, move := range moves {
		stats := s.state.ShardStats[move.ShardID]
		stats.LastMoveTime = s.now
		s.state.ShardStats[move.ShardID] = stats
	}
	s.syncAssignmentsToState()
	s.recordSample(row.ShardLoads)
	return nil
}

func (s *Simulator) Result(rows int) Result {
	smoothedLoads := s.SmoothedExecutorLoads()
	reportedLoads := s.finalReportedLoads
	if reportedLoads == nil {
		reportedLoads = make(map[string]float64, len(s.assignments))
	}

	return Result{
		Rows:          rows,
		Shards:        s.shardCount(),
		Executors:     s.cfg.ExecutorCount,
		Moves:         s.moves,
		FinalCounts:   assignmentCounts(s.assignments),
		LastTimestamp: s.now,

		FinalSmoothedLoads:         smoothedLoads,
		FinalSmoothedMaxOverMean:   maxOverMean(smoothedLoads),
		FinalSmoothedCV:            coefficientOfVariation(smoothedLoads),
		AverageSmoothedMaxOverMean: divideBySamples(s.smoothedMaxOverMeanTotal, s.samples),
		AverageSmoothedCV:          divideBySamples(s.smoothedCVTotal, s.samples),
		WorstSmoothedMaxOverMean:   s.worstSmoothedMaxOverMean,
		WorstSmoothedCV:            s.worstSmoothedCV,

		FinalReportedLoads:         reportedLoads,
		FinalReportedMaxOverMean:   maxOverMean(reportedLoads),
		FinalReportedCV:            coefficientOfVariation(reportedLoads),
		AverageReportedMaxOverMean: divideBySamples(s.reportedMaxOverMeanTotal, s.samples),
		AverageReportedCV:          divideBySamples(s.reportedCVTotal, s.samples),
		WorstReportedMaxOverMean:   s.worstReportedMaxOverMean,
		WorstReportedCV:            s.worstReportedCV,
	}
}

func (s *Simulator) SmoothedExecutorLoads() map[string]float64 {
	loads := make(map[string]float64, len(s.assignments))
	for executorID, shards := range s.assignments {
		for _, shardID := range shards {
			loads[executorID] += s.state.ShardStats[shardID].SmoothedLoad
		}
	}
	return loads
}

func (s *Simulator) reportedExecutorLoads(shardLoads map[string]float64) map[string]float64 {
	loads := make(map[string]float64, len(s.assignments))
	for executorID, shards := range s.assignments {
		for _, shardID := range shards {
			loads[executorID] += shardLoads[shardID]
		}
	}
	return loads
}

func (s *Simulator) recordSample(reportedShardLoads map[string]float64) {
	smoothedLoads := s.SmoothedExecutorLoads()
	smoothedCV := coefficientOfVariation(smoothedLoads)
	smoothedMaxOverMean := maxOverMean(smoothedLoads)

	reportedLoads := s.reportedExecutorLoads(reportedShardLoads)
	reportedCV := coefficientOfVariation(reportedLoads)
	reportedMaxOverMean := maxOverMean(reportedLoads)

	s.samples++
	s.smoothedCVTotal += smoothedCV
	s.smoothedMaxOverMeanTotal += smoothedMaxOverMean
	s.reportedCVTotal += reportedCV
	s.reportedMaxOverMeanTotal += reportedMaxOverMean
	s.worstSmoothedCV = max(s.worstSmoothedCV, smoothedCV)
	s.worstSmoothedMaxOverMean = max(s.worstSmoothedMaxOverMean, smoothedMaxOverMean)
	s.worstReportedCV = max(s.worstReportedCV, reportedCV)
	s.worstReportedMaxOverMean = max(s.worstReportedMaxOverMean, reportedMaxOverMean)
	s.finalReportedLoads = reportedLoads
}

func Run(history []LoadHistoryRow, shardIDs []string, cfg Config) (Result, error) {
	if len(history) == 0 {
		return Result{}, fmt.Errorf("history must not be empty")
	}
	sim, err := New(cfg, history[0].Timestamp, shardIDs)
	if err != nil {
		return Result{}, err
	}
	for i, row := range history {
		if err := sim.ApplyRow(row); err != nil {
			return Result{}, fmt.Errorf("row %d: %w", i, err)
		}
	}
	return sim.Result(len(history)), nil
}

func (s *Simulator) syncAssignmentsToState() {
	for executorID, shards := range s.assignments {
		assignedState := s.state.ShardAssignments[executorID]
		assignedState.AssignedShards = make(map[string]*types.ShardAssignment, len(shards))
		for _, shardID := range shards {
			assignedState.AssignedShards[shardID] = &types.ShardAssignment{Status: types.AssignmentStatusREADY}
		}
		assignedState.LastUpdated = s.now
		s.state.ShardAssignments[executorID] = assignedState
	}
}

func (s *Simulator) shardCount() int {
	total := 0
	for _, shards := range s.assignments {
		total += len(shards)
	}
	return total
}

func applyMoves(assignments map[string][]string, moves []plan.Move) error {
	for _, move := range moves {
		idx := slices.Index(assignments[move.From], move.ShardID)
		if idx == -1 {
			return fmt.Errorf("shard %s not found in source executor %s", move.ShardID, move.From)
		}
		assignments[move.From][idx] = assignments[move.From][len(assignments[move.From])-1]
		assignments[move.From] = assignments[move.From][:len(assignments[move.From])-1]
		assignments[move.To] = append(assignments[move.To], move.ShardID)
	}
	return nil
}

func greedyConfig(cfg Config) config.LoadBalancingGreedyConfig {
	return config.LoadBalancingGreedyConfig{
		PerShardCooldown: func(namespace string) time.Duration {
			return cfg.PerShardCooldown
		},
		MoveBudgetProportion: func(namespace string) float64 {
			return cfg.MoveBudgetProportion
		},
		HysteresisUpperBand: func(namespace string) float64 {
			return cfg.HysteresisUpperBand
		},
		HysteresisLowerBand: func(namespace string) float64 {
			return cfg.HysteresisLowerBand
		},
		SevereImbalanceRatio: func(namespace string) float64 {
			return cfg.SevereImbalanceRatio
		},
	}
}

func (cfg Config) withDefaults() Config {
	if cfg.Namespace == "" {
		cfg.Namespace = "loadbalancer-simulation"
	}
	if cfg.ExecutorCount == 0 {
		cfg.ExecutorCount = 4
	}
	if cfg.PerShardCooldown == 0 {
		cfg.PerShardCooldown = 30 * time.Second
	}
	if cfg.MoveBudgetProportion == 0 {
		cfg.MoveBudgetProportion = 0.01
	}
	if cfg.HysteresisUpperBand == 0 {
		cfg.HysteresisUpperBand = 1.15
	}
	if cfg.HysteresisLowerBand == 0 {
		cfg.HysteresisLowerBand = 0.90
	}
	if cfg.SevereImbalanceRatio == 0 {
		cfg.SevereImbalanceRatio = 1.3
	}
	return cfg
}

func assignmentCounts(assignments map[string][]string) map[string]int {
	counts := make(map[string]int, len(assignments))
	for executorID, shards := range assignments {
		counts[executorID] = len(shards)
	}
	return counts
}

func divideBySamples(total float64, samples int) float64 {
	if samples == 0 {
		return 0
	}
	return total / float64(samples)
}

func maxOverMean(loads map[string]float64) float64 {
	if len(loads) == 0 {
		return 0
	}
	maxLoad := 0.0
	total := 0.0
	for _, load := range loads {
		total += load
		if load > maxLoad {
			maxLoad = load
		}
	}
	mean := total / float64(len(loads))
	if mean == 0 {
		return 0
	}
	return maxLoad / mean
}

func coefficientOfVariation(loads map[string]float64) float64 {
	if len(loads) == 0 {
		return 0
	}
	total := 0.0
	for _, load := range loads {
		total += load
	}
	mean := total / float64(len(loads))
	if mean == 0 {
		return 0
	}
	var variance float64
	for _, load := range loads {
		d := load - mean
		variance += d * d
	}
	return math.Sqrt(variance/float64(len(loads))) / mean
}
