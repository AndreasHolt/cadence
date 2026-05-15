// balance-grid runs a parameter sweep over move-penalty-coefficient values
// through the Cadence shard-distributor greedy load-balance algorithm.
//
// Usage:
//
//	go run ./cmd/balance-grid -csv <file> [flags]
//
// Outputs:
//
//	grid_results.csv
//
//	Columns: move_scoring_mode, move_penalty_coefficient, total_moves, total_load_moved,
//
//	avg_mm_smooth, worst_mm_smooth, avg_mm_reported, worst_mm_reported,
//	avg_cv_smooth, worst_cv_smooth, avg_cv_reported, worst_cv_reported
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/config"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/plan"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/strategy/greedy"
	"github.com/uber/cadence/service/sharddistributor/statistics"
	"github.com/uber/cadence/service/sharddistributor/store"
)

type combo struct {
	moveScoringMode        string
	movePenaltyCoefficient float64
}

func (c combo) toRow() []string {
	return []string{
		c.moveScoringMode,
		strconv.FormatFloat(c.movePenaltyCoefficient, 'f', 4, 64),
	}
}

type result struct {
	cb              combo
	totalMoves      int
	totalLoadMoved  float64
	avgMMSmooth     float64
	worstMMSmooth   float64
	avgMMReported   float64
	worstMMReported float64
	avgCVSmooth     float64
	worstCVSmooth   float64
	avgCVReported   float64
	worstCVReported float64
}

func (r result) toRow() []string {
	row := r.cb.toRow()
	metrics := []string{
		strconv.Itoa(r.totalMoves),
		strconv.FormatFloat(r.totalLoadMoved, 'f', 2, 64),
		strconv.FormatFloat(r.avgMMSmooth, 'f', 6, 64),
		strconv.FormatFloat(r.worstMMSmooth, 'f', 6, 64),
		strconv.FormatFloat(r.avgMMReported, 'f', 6, 64),
		strconv.FormatFloat(r.worstMMReported, 'f', 6, 64),
		strconv.FormatFloat(r.avgCVSmooth, 'f', 6, 64),
		strconv.FormatFloat(r.worstCVSmooth, 'f', 6, 64),
		strconv.FormatFloat(r.avgCVReported, 'f', 6, 64),
		strconv.FormatFloat(r.worstCVReported, 'f', 6, 64),
	}
	return append(row, metrics...)
}

func main() {
	var (
		csvPath           string
		outPath           string
		numExecutors      int
		rebalanceInterval time.Duration
		loadInterval      time.Duration
		moveBudget        float64
		cooldown          time.Duration
		upperBand         float64
		lowerBand         float64
		severeRatio       float64
	)

	flag.StringVar(&csvPath, "csv", "", "Path to input CSV file (required)")
	flag.StringVar(&outPath, "out", "grid_results.csv", "Output CSV path")
	flag.IntVar(&numExecutors, "executors", 4, "Number of simulated executors")
	flag.DurationVar(&rebalanceInterval, "rebalance-interval", 2*time.Second, "Simulated time between rebalance passes")
	flag.DurationVar(&loadInterval, "load-interval", 10*time.Second, "Simulated time between CSV row advances")
	flag.Float64Var(&moveBudget, "move-budget", 0.01, "Fraction of shards that may move per rebalance pass")
	flag.DurationVar(&cooldown, "cooldown", 30*time.Second, "Per-shard move cooldown")
	flag.Float64Var(&upperBand, "upper-band", 1.15, "Hysteresis upper-band multiplier")
	flag.Float64Var(&lowerBand, "lower-band", 0.90, "Hysteresis lower-band multiplier")
	flag.Float64Var(&severeRatio, "severe-ratio", 1.3, "Severe-imbalance escape-hatch ratio")
	flag.Parse()

	if csvPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	zapCfg := zap.NewDevelopmentConfig()
	zapCfg.OutputPaths = []string{"stderr"}
	zapCfg.ErrorOutputPaths = []string{"stderr"}

	if numExecutors < 1 {
		fmt.Fprintf(os.Stderr, "-executors must be >= 1\n")
		os.Exit(1)
	}

	// ── Load history ─────────────────────────────────────────────────────────
	f, err := os.Open(csvPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open CSV: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	history, shardIDs, err := loadCSVHistory(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse CSV: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d rows, %d shards from %s\n", len(history), len(shardIDs), csvPath)

	// ── Build combo list ───────────────────────────────────────────────────
	scoringModes := []string{"benefit", "cost_aware"}
	fixedCosts := []float64{0.0}
	i := 0.02
	for i < 1.0 {
		fixedCosts = append(fixedCosts, i)
		i += 0.02
	}

	var combos []combo
	for _, mode := range scoringModes {
		for _, fixed := range fixedCosts {
			combos = append(combos, combo{
				moveScoringMode:        mode,
				movePenaltyCoefficient: fixed,
			})
		}
	}

	fmt.Printf("Running %d grid permutations across %d workers\n", len(combos), runtime.NumCPU())

	jobs := make(chan combo, len(combos))
	results := make(chan result, len(combos))
	var wg sync.WaitGroup

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cb := range jobs {
				res, err := runGridSimulation(
					cb, history, shardIDs, numExecutors,
					rebalanceInterval, loadInterval,
					moveBudget, cooldown, upperBand, lowerBand, severeRatio,
				)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Simulation failed for combo %v: %v\n", cb, err)
					continue
				}
				results <- res
			}
		}()
	}

	for _, cb := range combos {
		jobs <- cb
	}
	close(jobs)

	// Wait and collect
	go func() {
		wg.Wait()
		close(results)
	}()

	outFile, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	w := csv.NewWriter(outFile)
	header := []string{
		"move_scoring_mode", "move_penalty_coefficient",
		"total_moves", "total_load_moved",
		"avg_mm_smooth", "worst_mm_smooth",
		"avg_mm_reported", "worst_mm_reported",
		"avg_cv_smooth", "worst_cv_smooth",
		"avg_cv_reported", "worst_cv_reported",
	}
	w.Write(header)

	count := 0
	for res := range results {
		w.Write(res.toRow())
		count++
		if count%10 == 0 {
			fmt.Printf("Completed %d / %d runs\n", count, len(combos))
		}
	}
	w.Flush()

	absOut, _ := filepath.Abs(outPath)
	fmt.Printf("Grid search complete. Results saved to %s\n", absOut)
}

func runGridSimulation(
	cb combo,
	history []loadHistoryRow,
	shardIDs []string,
	numExecutors int,
	rebalanceInterval time.Duration,
	loadInterval time.Duration,
	moveBudget float64,
	cooldown time.Duration,
	upperBand float64,
	lowerBand float64,
	severeRatio float64,
) (result, error) {

	executors := make([]string, numExecutors)
	for i := range executors {
		executors[i] = fmt.Sprintf("exec-%d", i)
	}

	cfg := config.LoadBalancingGreedyConfig{
		PerShardCooldown:       func(string) time.Duration { return cooldown },
		MoveBudgetProportion:   func(string) float64 { return moveBudget },
		HysteresisUpperBand:    func(string) float64 { return upperBand },
		HysteresisLowerBand:    func(string) float64 { return lowerBand },
		SevereImbalanceRatio:   func(string) float64 { return severeRatio },
		HeterogeneityMode:      func(string) string { return config.GreedyHeterogeneityModeOff },
		MoveScoringMode:        func(string) string { return cb.moveScoringMode },
		MovePenaltyCoefficient: func(string) float64 { return cb.movePenaltyCoefficient },
		CPUSecondsSmoothingTau: func(string) time.Duration { return 5 * time.Minute },
	}

	assignments := make(map[string][]string)
	shardStats := make(map[string]store.ShardStatistics)
	assignedState := make(map[string]store.AssignedState)
	executorsMap := make(map[string]store.HeartbeatState)
	now := history[0].Timestamp

	for _, e := range executors {
		assignments[e] = nil
		executorsMap[e] = store.HeartbeatState{
			Status:         types.ExecutorStatusACTIVE,
			LastHeartbeat:  now,
			ReportedShards: make(map[string]*types.ShardStatusReport),
		}
		assignedState[e] = store.AssignedState{
			AssignedShards: make(map[string]*types.ShardAssignment),
			LastUpdated:    now,
		}
	}
	for i, shard := range shardIDs {
		e := executors[i%len(executors)]
		assignments[e] = append(assignments[e], shard)
		shardStats[shard] = store.ShardStatistics{LastUpdateTime: time.Time{}}
		assignedState[e].AssignedShards[shard] = &types.ShardAssignment{Status: types.AssignmentStatusREADY}
	}

	ns := &store.NamespaceState{
		Executors:        executorsMap,
		ShardStats:       shardStats,
		ShardAssignments: assignedState,
	}

	for shardID, load := range history[0].ShardLoads {
		stats := ns.ShardStats[shardID]
		stats.SmoothedLoad = load
		stats.LastUpdateTime = now
		ns.ShardStats[shardID] = stats
	}

	currentHistoryIdx := 0
	nextLoadUpdate := history[0].Timestamp.Add(loadInterval)
	tickCount := 0
	totalMoves := 0
	totalLoadMoved := 0.0

	var (
		sumMMSmooth, sumMMRaw     float64
		sumCVSmooth, sumCVRaw     float64
		worstMMSmooth, worstMMRaw float64
		worstCVSmooth, worstCVRaw float64
	)

	for {
		currentTime := now

		if currentTime.After(nextLoadUpdate) && currentHistoryIdx < len(history)-1 {
			currentHistoryIdx++
			row := history[currentHistoryIdx]
			if row.Timestamp.After(now) {
				now = row.Timestamp
			}
			for shardID, load := range row.ShardLoads {
				stats := ns.ShardStats[shardID]
				smoothed, _ := statistics.CalculateSmoothedLoad(
					stats.SmoothedLoad, load, stats.LastUpdateTime, now,
				)
				stats.SmoothedLoad = smoothed
				stats.LastUpdateTime = now
				ns.ShardStats[shardID] = stats
			}
			nextLoadUpdate = nextLoadUpdate.Add(loadInterval)
		}

		moves, err := greedy.PlanRebalance(cfg, "sim", ns, assignments, now, 0, metrics.NoopScope)
		if err != nil {
			return result{}, err
		}
		applyMoves(assignments, ns, moves)
		totalMoves += len(moves)
		for _, m := range moves {
			if s, ok := ns.ShardStats[m.ShardID]; ok {
				totalLoadMoved += s.SmoothedLoad
			}
		}

		loadsSmooth := computeLoads(assignments, ns)
		loadsRaw := make(map[string]float64)
		curRawLoads := history[currentHistoryIdx].ShardLoads
		for execID, shards := range assignments {
			var rawSum float64
			for _, shardID := range shards {
				rawSum += curRawLoads[shardID]
			}
			loadsRaw[execID] = rawSum
		}

		maxSmooth, meanSmooth, _, cvSmooth := stats(loadsSmooth)
		maxRaw, meanRaw, _, cvRaw := stats(loadsRaw)

		var maxMeanSmooth, maxMeanRaw float64
		if meanSmooth > 0 {
			maxMeanSmooth = maxSmooth / meanSmooth
		}
		if meanRaw > 0 {
			maxMeanRaw = maxRaw / meanRaw
		}

		tickCount++
		sumMMSmooth += maxMeanSmooth
		sumMMRaw += maxMeanRaw
		sumCVSmooth += cvSmooth
		sumCVRaw += cvRaw

		if maxMeanSmooth > worstMMSmooth {
			worstMMSmooth = maxMeanSmooth
		}
		if maxMeanRaw > worstMMRaw {
			worstMMRaw = maxMeanRaw
		}
		if cvSmooth > worstCVSmooth {
			worstCVSmooth = cvSmooth
		}
		if cvRaw > worstCVRaw {
			worstCVRaw = cvRaw
		}

		if currentHistoryIdx >= len(history)-1 && currentTime.After(nextLoadUpdate) {
			break
		}

		now = now.Add(rebalanceInterval)
	}

	res := result{cb: cb, totalMoves: totalMoves, totalLoadMoved: totalLoadMoved}
	if tickCount > 0 {
		res.avgMMSmooth = sumMMSmooth / float64(tickCount)
		res.avgMMReported = sumMMRaw / float64(tickCount)
		res.avgCVSmooth = sumCVSmooth / float64(tickCount)
		res.avgCVReported = sumCVRaw / float64(tickCount)
	}
	res.worstMMSmooth = worstMMSmooth
	res.worstMMReported = worstMMRaw
	res.worstCVSmooth = worstCVSmooth
	res.worstCVReported = worstCVRaw

	return res, nil
}

// ── Shared helpers (copied from balance-sim) ──────────────────────────────

type loadHistoryRow struct {
	Timestamp  time.Time
	ShardLoads map[string]float64
}

func loadCSVHistory(f *os.File) ([]loadHistoryRow, []string, error) {
	cr := csv.NewReader(f)
	cr.TrimLeadingSpace = true

	const timeFormat = "2006-01-02 15:04:05"

	var rows []loadHistoryRow
	var shardIDs []string
	shardIDsInitialised := false

	for {
		record, err := cr.Read()
		if err != nil {
			break
		}
		if len(record) < 2 {
			continue
		}

		ts, err := time.Parse(timeFormat, record[0])
		if err != nil {
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
				load = 0
			}
			if i < len(shardIDs) {
				shardLoads[shardIDs[i]] = load
			}
		}

		rows = append(rows, loadHistoryRow{
			Timestamp:  ts,
			ShardLoads: shardLoads,
		})
	}

	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("no valid data rows found in CSV")
	}

	return rows, shardIDs, nil
}

func applyMoves(assignments map[string][]string, ns *store.NamespaceState, moves []plan.Move) {
	for _, m := range moves {
		src := assignments[m.From]
		for i, s := range src {
			if s == m.ShardID {
				assignments[m.From] = append(src[:i], src[i+1:]...)
				break
			}
		}
		assignments[m.To] = append(assignments[m.To], m.ShardID)
		delete(ns.ShardAssignments[m.From].AssignedShards, m.ShardID)
		ns.ShardAssignments[m.To].AssignedShards[m.ShardID] = &types.ShardAssignment{Status: types.AssignmentStatusREADY}
	}
}

func computeLoads(assignments map[string][]string, ns *store.NamespaceState) map[string]float64 {
	loads := make(map[string]float64, len(assignments))
	for executorID, shards := range assignments {
		for _, shardID := range shards {
			if s, ok := ns.ShardStats[shardID]; ok {
				loads[executorID] += s.SmoothedLoad
			}
		}
	}
	return loads
}

func stats(loads map[string]float64) (maxLoad, mean, stdDev, cv float64) {
	if len(loads) == 0 {
		return 0, 0, 0, 0
	}
	for _, v := range loads {
		mean += v
		if v > maxLoad {
			maxLoad = v
		}
	}
	mean /= float64(len(loads))
	for _, v := range loads {
		d := v - mean
		stdDev += d * d
	}
	stdDev = math.Sqrt(stdDev / float64(len(loads)))
	if mean > 0 {
		cv = stdDev / mean
	}
	return maxLoad, mean, stdDev, cv
}

func printProgress(tick, total, historyIdx, historyTotal int) {
	const barWidth = 30
	pct := tick * 100 / total
	if pct > 100 {
		pct = 100
	}
	filled := tick * barWidth / total
	if filled > barWidth {
		filled = barWidth
	}
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
	fmt.Printf("\r[%s] %3d%% | tick %d/%d | history %d/%d", bar, pct, tick, total, historyIdx+1, historyTotal)
}
