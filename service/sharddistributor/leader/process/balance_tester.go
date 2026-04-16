package process

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/config"
	"github.com/uber/cadence/service/sharddistributor/statistics"
	"github.com/uber/cadence/service/sharddistributor/store"
)

// LoadHistoryRow is a single timestamped snapshot of per-shard load values.
type LoadHistoryRow struct {
	Timestamp  time.Time
	ShardLoads map[string]float64
}

// BalanceTester runs the real greedy load-balance algorithm against an in-memory
// namespace state, with a deterministic simulated clock. It is intentionally
// kept free of any store, logger, election, or metrics dependencies so it can
// be used in isolated unit/simulation tests.
type BalanceTester struct {
	processor      *namespaceProcessor
	timeSource     clock.MockedTimeSource
	namespaceState *store.NamespaceState
	assignments    map[string][]string
	deletedShards  map[string]store.ShardState
}

func NewBalanceTester(
	startTime time.Time,
	namespaceCfg config.Namespace,
	loadBalanceCfg config.LoadBalance,
) *BalanceTester {
	timeSource := clock.NewMockedTimeSourceAt(startTime)

	perShardCooldown := loadBalanceCfg.PerShardCooldown
	if perShardCooldown <= 0 {
		perShardCooldown = time.Minute
	}
	moveBudgetProportion := loadBalanceCfg.MoveBudgetProportion
	if moveBudgetProportion <= 0 {
		moveBudgetProportion = 0.01
	}
	hysteresisUpperBand := loadBalanceCfg.HysteresisUpperBand
	if hysteresisUpperBand <= 0 {
		hysteresisUpperBand = 1.15
	}
	hysteresisLowerBand := loadBalanceCfg.HysteresisLowerBand
	if hysteresisLowerBand <= 0 {
		hysteresisLowerBand = 0.95
	}
	severeImbalanceRatio := loadBalanceCfg.SevereImbalanceRatio
	if severeImbalanceRatio <= 0 {
		severeImbalanceRatio = 1.5
	}

	// Construct a minimal namespaceProcessor: the real loadBalance method only
	// requires cfg.LoadBalance.* fields, namespaceCfg (for getShards), and
	// timeSource (for now := p.timeSource.Now()). All other fields (logger,
	// store, election, metrics) are left as their zero values and are not
	// touched by the load-balance code path.
	proc := &namespaceProcessor{
		namespaceCfg: namespaceCfg,
		timeSource:   timeSource,
		cfg: config.LeaderProcess{
			LoadBalance: config.LoadBalance{
				PerShardCooldown:     perShardCooldown,
				MoveBudgetProportion: moveBudgetProportion,
				HysteresisUpperBand:  hysteresisUpperBand,
				HysteresisLowerBand:  hysteresisLowerBand,
				SevereImbalanceRatio: severeImbalanceRatio,
			},
		},
	}

	return &BalanceTester{
		processor:  proc,
		timeSource: timeSource,
		namespaceState: &store.NamespaceState{
			Executors:        make(map[string]store.HeartbeatState),
			ShardStats:       make(map[string]store.ShardStatistics),
			ShardAssignments: make(map[string]store.AssignedState),
		},
		assignments:   make(map[string][]string),
		deletedShards: make(map[string]store.ShardState),
	}
}

// SetInitialAssignments distributes shards round-robin across executors and
// initialises the namespace state accordingly.
func (bt *BalanceTester) SetInitialAssignments(executors []string, shards []string) {
	for _, executor := range executors {
		bt.assignments[executor] = []string{}
		bt.namespaceState.Executors[executor] = store.HeartbeatState{
			Status:         types.ExecutorStatusACTIVE,
			LastHeartbeat:  bt.timeSource.Now(),
			ReportedShards: make(map[string]*types.ShardStatusReport),
		}
		bt.namespaceState.ShardAssignments[executor] = store.AssignedState{
			AssignedShards: make(map[string]*types.ShardAssignment),
			LastUpdated:    bt.timeSource.Now(),
		}
	}

	for i, shard := range shards {
		executor := executors[i%len(executors)]
		bt.assignments[executor] = append(bt.assignments[executor], shard)
		bt.namespaceState.ShardStats[shard] = store.ShardStatistics{
			SmoothedLoad:   0,
			LastUpdateTime: time.Time{},
		}
		bt.namespaceState.ShardAssignments[executor].AssignedShards[shard] = &types.ShardAssignment{
			Status: types.AssignmentStatusREADY,
		}
	}
}

// ApplyLoad advances the clock to the row's timestamp and updates the
// smoothed load for each shard using an exponential moving average (alpha=0.2).
func (bt *BalanceTester) ApplyLoad(row LoadHistoryRow) {
	d := row.Timestamp.Sub(bt.timeSource.Now())
	if d > 0 {
		bt.timeSource.Advance(d)
	}

	for shardID, load := range row.ShardLoads {
		stats, ok := bt.namespaceState.ShardStats[shardID]
		if !ok {
			stats = store.ShardStatistics{LastUpdateTime: time.Time{}}
		}

		stats.SmoothedLoad = statistics.CalculateSmoothedLoad(
			stats.SmoothedLoad,
			load,
			stats.LastUpdateTime,
			bt.timeSource.Now(),
		)
		stats.LastUpdateTime = bt.timeSource.Now()
		bt.namespaceState.ShardStats[shardID] = stats
	}
}

// Rebalance runs one pass of the real greedy load-balance algorithm and
// updates bt.assignments and bt.namespaceState.ShardAssignments to reflect
// any moves that were made.
// It returns the number of shards moved.
func (bt *BalanceTester) Rebalance() (int, error) {
	prevLoc := make(map[string]string)
	for exec, shards := range bt.assignments {
		for _, shard := range shards {
			prevLoc[shard] = exec
		}
	}

	moved, err := bt.processor.loadBalance(bt.assignments, bt.namespaceState, bt.deletedShards, nil)
	if err != nil {
		return 0, err
	}
	moves := 0
	if moved {
		bt.syncAssignmentsToState()
		for exec, shards := range bt.assignments {
			for _, shard := range shards {
				if prevLoc[shard] != "" && prevLoc[shard] != exec {
					moves++
					stats := bt.namespaceState.ShardStats[shard]
					stats.LastMoveTime = bt.timeSource.Now()
					bt.namespaceState.ShardStats[shard] = stats
				}
			}
		}
	}
	return moves, nil
}

// syncAssignmentsToState writes bt.assignments back into namespaceState.ShardAssignments
// so the state stays consistent with the working assignments map after each rebalance.
func (bt *BalanceTester) syncAssignmentsToState() {
	now := bt.timeSource.Now()
	for executorID, shards := range bt.assignments {
		assignedState, exists := bt.namespaceState.ShardAssignments[executorID]
		if !exists {
			assignedState = store.AssignedState{
				AssignedShards: make(map[string]*types.ShardAssignment),
			}
		}
		assignedState.AssignedShards = make(map[string]*types.ShardAssignment, len(shards))
		for _, shard := range shards {
			assignedState.AssignedShards[shard] = &types.ShardAssignment{
				Status: types.AssignmentStatusREADY,
			}
		}
		assignedState.LastUpdated = now
		bt.namespaceState.ShardAssignments[executorID] = assignedState
	}
}

func (bt *BalanceTester) GetAssignments() map[string][]string {
	return bt.assignments
}

func (bt *BalanceTester) GetTimeSource() clock.MockedTimeSource {
	return bt.timeSource
}

func (bt *BalanceTester) GetNamespaceState() *store.NamespaceState {
	return bt.namespaceState
}

// ComputeLoads returns the current smoothed load total for each executor,
// derived from bt.assignments and the ShardStats in bt.namespaceState.
func (bt *BalanceTester) ComputeLoads() map[string]float64 {
	loads, _ := computeExecutorLoads(bt.assignments, bt.namespaceState)
	return loads
}

// RunBalanceSimulation replays a load history through the greedy balancer and
// returns the final shard assignments.
//
//   - rebalanceInterval is how much simulated time passes between rebalance passes.
//   - loadUpdateInterval is how much simulated time between loading successive history rows.
func RunBalanceSimulation(
	history []LoadHistoryRow,
	executors []string,
	shards []string,
	namespaceCfg config.Namespace,
	loadBalanceCfg config.LoadBalance,
	rebalanceInterval time.Duration,
	loadUpdateInterval time.Duration,
) (map[string][]string, error) {
	if len(history) == 0 {
		return nil, fmt.Errorf("history must not be empty")
	}

	tester := NewBalanceTester(history[0].Timestamp, namespaceCfg, loadBalanceCfg)
	tester.SetInitialAssignments(executors, shards)

	currentHistoryIdx := 0
	nextLoadUpdate := history[0].Timestamp.Add(loadUpdateInterval)

	for {
		currentTime := tester.GetTimeSource().Now()

		// Advance to the next history row when it is due.
		if currentTime.After(nextLoadUpdate) && currentHistoryIdx < len(history)-1 {
			currentHistoryIdx++
			tester.ApplyLoad(history[currentHistoryIdx])
			nextLoadUpdate = nextLoadUpdate.Add(loadUpdateInterval)
		}

		if _, err := tester.Rebalance(); err != nil {
			return nil, fmt.Errorf("rebalance at index %d: %w", currentHistoryIdx, err)
		}

		// Stop after we have consumed all history rows and run one final rebalance.
		if currentHistoryIdx >= len(history)-1 && currentTime.After(nextLoadUpdate) {
			break
		}

		tester.GetTimeSource().Advance(rebalanceInterval)
	}

	return tester.GetAssignments(), nil
}

// LoadCSVHistory reads a CSV file in the format:
//
//	<timestamp>, <shard_0_load>, <shard_1_load>, ...
//
// The first row is expected to be a data row (no header). Shard IDs are
// generated as "0", "1", "2", … matching the column index (after the timestamp
// column). The timestamp format is "2006-01-02 15:04:05".
//
// If the file has a header row (first column is not parseable as a timestamp),
// it is silently skipped.
func LoadCSVHistory(r io.Reader) ([]LoadHistoryRow, []string, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true

	const timeFormat = "2006-01-02 15:04:05"

	var rows []LoadHistoryRow
	var shardIDs []string
	shardIDsInitialised := false

	for {
		record, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("csv read: %w", err)
		}
		if len(record) < 2 {
			continue
		}

		ts, err := time.Parse(timeFormat, record[0])
		if err != nil {
			// Likely a header row — skip it.
			continue
		}

		if !shardIDsInitialised {
			shardIDs = make([]string, len(record)-1)
			for i := range shardIDs {
				shardIDs[i] = strconv.Itoa(i)
			}
			shardIDsInitialised = true
		}

		shardLoads := make(map[string]float64, len(record)-1)
		for i, val := range record[1:] {
			load, err := strconv.ParseFloat(val, 64)
			if err != nil {
				// Missing or malformed value — treat as zero load.
				load = 0
			}
			if i < len(shardIDs) {
				shardLoads[shardIDs[i]] = load
			}
		}

		rows = append(rows, LoadHistoryRow{
			Timestamp:  ts,
			ShardLoads: shardLoads,
		})
	}

	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("no valid data rows found in CSV")
	}

	return rows, shardIDs, nil
}
