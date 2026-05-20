// balance-grid runs a parameter sweep over hysteresis bands, severe-imbalance
// ratio, cooldown, move-budget, smoothing tau and load-source (smoothed vs
// reported) through the Cadence shard-distributor greedy load-balance algorithm.
//
// Swaps are disabled and the move-penalty coefficient is fixed at 0 for all
// runs.
//
// Usage:
//
//	go run ./cmd/balance-grid -csv <file> [flags]
//
// Outputs:
//
//	grid_results.csv
//
//	Columns: upper_band, lower_band, severe_ratio, cooldown_ms, move_budget,
//	         tau_ms, use_smoothed_load,
//	         total_moves, total_load_moved,
//	         avg_mm_smooth, worst_mm_smooth, avg_mm_reported, worst_mm_reported,
//	         avg_cv_smooth, worst_cv_smooth, avg_cv_reported, worst_cv_reported
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

	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/config"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/plan"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/strategy/greedy"
	"github.com/uber/cadence/service/sharddistributor/statistics"
	"github.com/uber/cadence/service/sharddistributor/store"
)

type combo struct {
	upperBand       float64
	lowerBand       float64
	severeRatio     float64
	cooldown        time.Duration
	moveBudget      float64
	tau             time.Duration
	useSmoothedLoad bool
}

func (c combo) toRow() []string {
	return []string{
		strconv.FormatFloat(c.upperBand, 'f', 4, 64),
		strconv.FormatFloat(c.lowerBand, 'f', 4, 64),
		strconv.FormatFloat(c.severeRatio, 'f', 4, 64),
		strconv.FormatInt(c.cooldown.Milliseconds(), 10),
		strconv.FormatFloat(c.moveBudget, 'f', 4, 64),
		strconv.FormatInt(c.tau.Milliseconds(), 10),
		strconv.FormatBool(c.useSmoothedLoad),
	}
}

func (c combo) key() string {
	return strings.Join(c.toRow(), "|")
}

func loadExistingResults(path string) (map[string]struct{}, error) {
	seen := make(map[string]struct{})
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return seen, nil
		}
		return nil, err
	}
	defer f.Close()

	cr := csv.NewReader(f)
	// skip header
	_, err = cr.Read()
	if err != nil {
		return seen, nil
	}

	for {
		rec, err := cr.Read()
		if err != nil {
			break
		}
		if len(rec) >= 7 {
			seen[strings.Join(rec[:7], "|")] = struct{}{}
		}
	}
	return seen, nil
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
	)

	flag.StringVar(&csvPath, "csv", "", "Path to input CSV file (required)")
	flag.StringVar(&outPath, "out", "grid_results.csv", "Output CSV path")
	flag.IntVar(&numExecutors, "executors", 4, "Number of simulated executors")
	flag.DurationVar(&rebalanceInterval, "rebalance-interval", 2*time.Second, "Simulated time between rebalance passes")
	flag.DurationVar(&loadInterval, "load-interval", 10*time.Second, "Simulated time between CSV row advances")
	flag.Parse()

	if csvPath == "" {
		flag.Usage()
		os.Exit(1)
	}

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
	upperBands := []float64{1.03, 1.05, 1.10, 1.15, 1.20, 1.25, 1.30, 1.35, 1.40}
	lowerBands := []float64{0.70, 0.80, 0.85, 0.90, 0.95, 0.98}
	severeRatios := []float64{1.3, 1.5, 1.7}
	cooldowns := []time.Duration{0, 30 * time.Second, 60 * time.Second, 120 * time.Second}
	moveBudgets := []float64{0.005, 0.01}
	taus := []time.Duration{0, 30 * time.Second, time.Minute, 5 * time.Minute}
	useSmoothedLoads := []bool{true, false}

	var combos []combo
	for _, ub := range upperBands {
		for _, lb := range lowerBands {
			for _, sr := range severeRatios {
				for _, cd := range cooldowns {
					for _, mb := range moveBudgets {
						for _, tau := range taus {
							for _, usl := range useSmoothedLoads {
								combos = append(combos, combo{
									upperBand:       ub,
									lowerBand:       lb,
									severeRatio:     sr,
									cooldown:        cd,
									moveBudget:      mb,
									tau:             tau,
									useSmoothedLoad: usl,
								})
							}
						}
					}
				}
			}
		}
	}

	existing, err := loadExistingResults(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read existing results: %v\n", err)
		os.Exit(1)
	}

	var filtered []combo
	for _, c := range combos {
		if _, ok := existing[c.key()]; !ok {
			filtered = append(filtered, c)
		}
	}
	if len(filtered) == 0 {
		fmt.Printf("All %d combinations already present in %s\n", len(combos), outPath)
		return
	}
	fmt.Printf("Found %d existing results, running %d new permutations across %d workers\n",
		len(existing), len(filtered), runtime.NumCPU())

	jobs := make(chan combo, len(filtered))
	results := make(chan result, len(filtered))
	var wg sync.WaitGroup

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cb := range jobs {
				res, err := runGridSimulation(
					cb, history, shardIDs, numExecutors,
					rebalanceInterval, loadInterval,
				)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Simulation failed for combo %v: %v\n", cb, err)
					continue
				}
				results <- res
			}
		}()
	}

	for _, cb := range filtered {
		jobs <- cb
	}
	close(jobs)

	// Wait and collect
	go func() {
		wg.Wait()
		close(results)
	}()

	info, err := os.Stat(outPath)
	appendMode := err == nil && info.Size() > 0

	openFlags := os.O_CREATE | os.O_WRONLY
	if appendMode {
		openFlags |= os.O_APPEND
	}
	outFile, err := os.OpenFile(outPath, openFlags, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	w := csv.NewWriter(outFile)
	header := []string{
		"upper_band", "lower_band", "severe_ratio", "cooldown_ms", "move_budget", "tau_ms", "use_smoothed_load",
		"total_moves", "total_load_moved",
		"avg_mm_smooth", "worst_mm_smooth",
		"avg_mm_reported", "worst_mm_reported",
		"avg_cv_smooth", "worst_cv_smooth",
		"avg_cv_reported", "worst_cv_reported",
	}
	if !appendMode {
		w.Write(header)
	}

	count := 0
	for res := range results {
		w.Write(res.toRow())
		count++
		if count%10 == 0 {
			fmt.Printf("Completed %d / %d new runs\n", count, len(filtered))
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
) (result, error) {

	executors := make([]string, numExecutors)
	for i := range executors {
		executors[i] = fmt.Sprintf("exec-%d", i)
	}

	cfg := config.LoadBalancingGreedyConfig{
		PerShardCooldown:       func(string) time.Duration { return cb.cooldown },
		MoveBudgetProportion:   func(string) float64 { return cb.moveBudget },
		HysteresisUpperBand:    func(string) float64 { return cb.upperBand },
		HysteresisLowerBand:    func(string) float64 { return cb.lowerBand },
		SevereImbalanceRatio:   func(string) float64 { return cb.severeRatio },
		HeterogeneityMode:      func(string) string { return config.GreedyHeterogeneityModeOff },
		MoveScoringMode:        func(string) string { return config.GreedyMoveScoringModeBenefit },
		MovePenaltyCoefficient: func(string) float64 { return 0.0 },
		CPUSecondsSmoothingTau: func(string) time.Duration { return cb.tau },
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
		if cb.useSmoothedLoad {
			shardStats[shard] = store.ShardStatistics{LastUpdateTime: time.Time{}}
		}
		assignedState[e].AssignedShards[shard] = &types.ShardAssignment{Status: types.AssignmentStatusREADY}
	}

	ns := &store.NamespaceState{
		Executors:        executorsMap,
		ShardStats:       shardStats,
		ShardAssignments: assignedState,
	}

	if cb.useSmoothedLoad {
		for shardID, load := range history[0].ShardLoads {
			stats := ns.ShardStats[shardID]
			stats.SmoothedLoad = load
			stats.LastUpdateTime = now
			ns.ShardStats[shardID] = stats
		}
	} else {
		for execID, shards := range assignments {
			for _, shardID := range shards {
				load := history[0].ShardLoads[shardID]
				ns.Executors[execID].ReportedShards[shardID] = &types.ShardStatusReport{
					Status:    types.ShardStatusREADY,
					ShardLoad: load,
				}
			}
		}
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
			if cb.useSmoothedLoad {
				for shardID, load := range row.ShardLoads {
					stats := ns.ShardStats[shardID]
					smoothed, _ := statistics.CalculateSmoothedLoad(
						stats.SmoothedLoad, load, stats.LastUpdateTime, now,
					)
					stats.SmoothedLoad = smoothed
					stats.LastUpdateTime = now
					ns.ShardStats[shardID] = stats
				}
			} else {
				for execID := range ns.Executors {
					exec := ns.Executors[execID]
					exec.ReportedShards = make(map[string]*types.ShardStatusReport)
					ns.Executors[execID] = exec
				}
				for execID, shards := range assignments {
					for _, shardID := range shards {
						load := row.ShardLoads[shardID]
						ns.Executors[execID].ReportedShards[shardID] = &types.ShardStatusReport{
							Status:    types.ShardStatusREADY,
							ShardLoad: load,
						}
					}
				}
			}
			nextLoadUpdate = nextLoadUpdate.Add(loadInterval)
		}

		moves, err := greedy.PlanRebalance(cfg, "sim", ns, assignments, now, 0, metrics.NoopScope)
		if err != nil {
			return result{}, err
		}
		applyMoves(assignments, ns, moves, now)
		totalMoves += len(moves)
		for _, m := range moves {
			totalLoadMoved += shardLoadForMove(ns, m, cb.useSmoothedLoad)
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

func applyMoves(assignments map[string][]string, ns *store.NamespaceState, moves []plan.Move, now time.Time) {
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

		// Update cooldown tracking.
		if stats, ok := ns.ShardStats[m.ShardID]; ok {
			stats.LastMoveTime = now
			ns.ShardStats[m.ShardID] = stats
		}

		// Move reported-shard entry if present.
		if report, ok := ns.Executors[m.From].ReportedShards[m.ShardID]; ok {
			ns.Executors[m.To].ReportedShards[m.ShardID] = report
			delete(ns.Executors[m.From].ReportedShards, m.ShardID)
		}
	}
}

func shardLoadForMove(ns *store.NamespaceState, m plan.Move, useSmoothedLoad bool) float64 {
	if useSmoothedLoad {
		if s, ok := ns.ShardStats[m.ShardID]; ok {
			return s.SmoothedLoad
		}
		return 0
	}
	if report, ok := ns.Executors[m.To].ReportedShards[m.ShardID]; ok {
		return report.ShardLoad
	}
	return 0
}

func computeLoads(assignments map[string][]string, ns *store.NamespaceState) map[string]float64 {
	loads := make(map[string]float64, len(assignments))
	for executorID, shards := range assignments {
		for _, shardID := range shards {
			if s, ok := ns.ShardStats[shardID]; ok {
				loads[executorID] += s.SmoothedLoad
			} else if report := ns.Executors[executorID].ReportedShards[shardID]; report != nil {
				loads[executorID] += report.ShardLoad
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
