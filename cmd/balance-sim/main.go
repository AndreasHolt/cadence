// balance-sim is a command-line tool for replaying real shard-load history
// through the Cadence shard-distributor greedy load-balance algorithm.
//
// Usage:
//
//	go run ./cmd/balance-sim -csv <file> [flags]
//
// Flags:
//
//	-csv string             Path to input CSV file (required).
//	                        Format: timestamp, shard_0_load, shard_1_load, ...
//	                        (no header row; first parseable-timestamp row wins).
//	-executors int          Number of simulated executors (default 4).
//	-out string             Output directory for metrics.csv (default ".").
//	-rebalance-interval     Simulated time between rebalance passes (default 2s).
//	-load-interval          Simulated time between CSV row advances (default 10s).
//	-move-budget float      Fraction of shards that may move per pass (default 0.01).
//	-cooldown duration      Per-shard move cooldown (default 1m).
//	-upper-band float       Hysteresis upper band multiplier (default 1.15).
//	-lower-band float       Hysteresis lower band multiplier (default 0.90).
//	-severe-ratio float     Severe-imbalance escape-hatch ratio (default 1.3).
//
// Outputs (in the specified directory):
//
//	smoothed_max_over_mean.csv
//	reported_max_over_mean.csv
//	smoothed_cv.csv
//	reported_cv.csv
//	moves_per_window.csv
//
// Each file contains columns: timestamp,value
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/config"
	"go.uber.org/zap"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/plan"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/strategy/greedy"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/strategy/optimal"
	"github.com/uber/cadence/service/sharddistributor/statistics"
	"github.com/uber/cadence/service/sharddistributor/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		csvPath           string
		numExecutors      int
		outDir            string
		rebalanceInterval time.Duration
		loadInterval      time.Duration
		moveBudget        float64
		cooldown          time.Duration
		upperBand         float64
		lowerBand         float64
		severeRatio       float64
		useOptimal        int
		enableSwap        bool
	)

	flag.StringVar(&csvPath, "csv", "", "Path to input CSV file (required)")
	flag.IntVar(&numExecutors, "executors", 4, "Number of simulated executors")
	flag.StringVar(&outDir, "out", ".", "Output directory for metrics.csv")
	flag.DurationVar(&rebalanceInterval, "rebalance-interval", 2*time.Second, "Simulated time between rebalance passes")
	flag.DurationVar(&loadInterval, "load-interval", 10*time.Second, "Simulated time between CSV row advances")
	flag.Float64Var(&moveBudget, "move-budget", 0.01, "Fraction of shards that may move per rebalance pass")
	flag.DurationVar(&cooldown, "cooldown", time.Minute, "Per-shard move cooldown")
	flag.Float64Var(&upperBand, "upper-band", 1.15, "Hysteresis upper-band multiplier")
	flag.Float64Var(&lowerBand, "lower-band", 0.90, "Hysteresis lower-band multiplier")
	flag.Float64Var(&severeRatio, "severe-ratio", 1.3, "Severe-imbalance escape-hatch ratio")
	flag.IntVar(&useOptimal, "optimal", 1, "Compare to optimal")
	flag.BoolVar(&enableSwap, "swap", true, "Enable pairwise shard swaps in greedy rebalancer")
	flag.Parse()

	if csvPath == "" {
		flag.Usage()
		return fmt.Errorf("-csv is required")
	}

	zapCfg := zap.NewDevelopmentConfig()
	zapCfg.OutputPaths = []string{"stderr"}
	zapCfg.ErrorOutputPaths = []string{"stderr"}
	zapLogger, err := zapCfg.Build()
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	logger := log.NewLogger(zapLogger)
	if numExecutors < 1 {
		return fmt.Errorf("-executors must be >= 1")
	}

	// ── Load history ─────────────────────────────────────────────────────────
	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("open CSV: %w", err)
	}
	defer f.Close()

	history, shardIDs, err := loadCSVHistory(f)
	if err != nil {
		return fmt.Errorf("parse CSV: %w", err)
	}
	fmt.Printf("Loaded %d rows, %d shards from %s\n", len(history), len(shardIDs), csvPath)

	// ── Build executor list ───────────────────────────────────────────────────
	executors := make([]string, numExecutors)
	for i := range executors {
		executors[i] = fmt.Sprintf("exec-%d", i)
	}

	// ── Configure greedy config (static wrappers) ────────────────────────────
	cfg := config.LoadBalancingGreedyConfig{
		PerShardCooldown:     func(string) time.Duration { return cooldown },
		MoveBudgetProportion: func(string) float64 { return moveBudget },
		HysteresisUpperBand:  func(string) float64 { return upperBand },
		HysteresisLowerBand:  func(string) float64 { return lowerBand },
		SevereImbalanceRatio: func(string) float64 { return severeRatio },
		EnableSwap:           enableSwap,
	}

	// ── Initialise namespace state ────────────────────────────────────────────
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

	// Apply the first history row immediately so initial loads are non-zero.
	for shardID, load := range history[0].ShardLoads {
		stats := ns.ShardStats[shardID]
		stats.SmoothedLoad = load
		stats.LastUpdateTime = now
		ns.ShardStats[shardID] = stats
	}

	// ── Open output files ─────────────────────────────────────────────────────
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	openCSV := func(name string) (*csv.Writer, func() error, error) {
		path := filepath.Join(outDir, name)
		f, err := os.Create(path)
		if err != nil {
			return nil, nil, err
		}
		w := csv.NewWriter(f)
		if err := w.Write([]string{"timestamp", "value"}); err != nil {
			return nil, nil, err
		}
		cleanup := func() error {
			w.Flush()
			if err := w.Error(); err != nil {
				return err
			}
			return f.Close()
		}
		return w, cleanup, nil
	}

	wSmoothMM, closeSmoothMM, err := openCSV("smoothed_max_over_mean.csv")
	if err != nil {
		return err
	}
	wRawMM, closeRawMM, err := openCSV("reported_max_over_mean.csv")
	if err != nil {
		return err
	}
	wSmoothCV, closeSmoothCV, err := openCSV("smoothed_cv.csv")
	if err != nil {
		return err
	}
	wRawCV, closeRawCV, err := openCSV("reported_cv.csv")
	if err != nil {
		return err
	}
	wMoves, closeMoves, err := openCSV("moves_per_window.csv")
	if err != nil {
		return err
	}
	wOptMM, closeOptMM, err := openCSV("optimal_max_over_mean.csv")
	if err != nil {
		return err
	}
	wOptCV, closeOptCV, err := openCSV("optimal_cv.csv")
	if err != nil {
		return err
	}
	wOptMoves, closeOptMoves, err := openCSV("optimal_moves.csv")
	if err != nil {
		return err
	}

	// ── Simulation loop ───────────────────────────────────────────────────────
	currentHistoryIdx := 0
	nextLoadUpdate := history[0].Timestamp.Add(loadInterval)
	tickCount := 0

	// Rough estimate of total ticks for the progress bar.
	ticksPerRow := int(loadInterval / rebalanceInterval)
	if ticksPerRow < 1 {
		ticksPerRow = 1
	}
	estimatedTotalTicks := (len(history) + 1) * ticksPerRow

	for {
		currentTime := now

		// Advance to the next history row when due.
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

		// Save state before greedy rebalances so optimal is computed from the same starting point.
		preGreedyAssignments := cloneAssignments(assignments)
		preGreedyRawLoads := history[currentHistoryIdx].ShardLoads

		moves, err := greedy.PlanRebalance(cfg, "sim", ns, assignments, now, logger, metrics.NoopScope)
		if err != nil {
			return fmt.Errorf("rebalance at tick %d: %w", tickCount, err)
		}
		applyMoves(assignments, ns, moves)

		// ── Optimal assignment from the pre-greedy starting point ───────────────
		var optAssignment optimal.Assignment
		var optMetrics optimal.Metrics
		var optMoves int
		if useOptimal == 1 {
			optAssignment = optimal.Solve(preGreedyRawLoads, executors, preGreedyAssignments)
			optMetrics = optimal.ComputeMetrics(optAssignment, preGreedyRawLoads)
			optMoves = optimal.DiffAssignments(preGreedyAssignments, optAssignment)
		}

		// Compute and record metrics.
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

		simTime := now.Format("2006-01-02 15:04:05")
		maxSmooth, meanSmooth, _, cvSmooth := stats(loadsSmooth)
		maxRaw, meanRaw, _, cvRaw := stats(loadsRaw)

		var maxMeanSmooth, maxMeanRaw float64
		if meanSmooth > 0 {
			maxMeanSmooth = maxSmooth / meanSmooth
		}
		if meanRaw > 0 {
			maxMeanRaw = maxRaw / meanRaw
		}

		writeRow := func(w *csv.Writer, val float64) error {
			return w.Write([]string{simTime, strconv.FormatFloat(val, 'f', 6, 64)})
		}

		if err := writeRow(wSmoothMM, maxMeanSmooth); err != nil {
			return fmt.Errorf("write smooth mm: %w", err)
		}
		if err := writeRow(wRawMM, maxMeanRaw); err != nil {
			return fmt.Errorf("write raw mm: %w", err)
		}
		if err := writeRow(wSmoothCV, cvSmooth); err != nil {
			return fmt.Errorf("write smooth cv: %w", err)
		}
		if err := writeRow(wRawCV, cvRaw); err != nil {
			return fmt.Errorf("write raw cv: %w", err)
		}
		if err := writeRow(wMoves, float64(len(moves))); err != nil {
			return fmt.Errorf("write moves: %w", err)
		}
		if useOptimal == 1 {
			if err := writeRow(wOptMM, optMetrics.MaxOverMean); err != nil {
				return fmt.Errorf("write optimal mm: %w", err)
			}
			if err := writeRow(wOptCV, optMetrics.CV); err != nil {
				return fmt.Errorf("write optimal cv: %w", err)
			}
			if err := writeRow(wOptMoves, float64(optMoves)); err != nil {
				return fmt.Errorf("write optimal moves: %w", err)
			}
		}
		tickCount++
		printProgress(tickCount, estimatedTotalTicks, currentHistoryIdx, len(history))

		// Terminate after all history is consumed and one final rebalance is done.
		if currentHistoryIdx >= len(history)-1 && currentTime.After(nextLoadUpdate) {
			break
		}

		now = now.Add(rebalanceInterval)
	}
	fmt.Println() // newline after progress bar

	for _, cleanup := range []func() error{
		closeSmoothMM, closeRawMM, closeSmoothCV, closeRawCV, closeMoves,
		closeOptMM, closeOptCV, closeOptMoves,
	} {
		if err := cleanup(); err != nil {
			return fmt.Errorf("close csv: %w", err)
		}
	}

	fmt.Printf("Wrote %d rows to output directory %s\n", tickCount, outDir)
	return nil
}

func cloneAssignments(a map[string][]string) map[string][]string {
	c := make(map[string][]string, len(a))
	for k, v := range a {
		cp := make([]string, len(v))
		copy(cp, v)
		c[k] = cp
	}
	return c
}

func applyMoves(assignments map[string][]string, ns *store.NamespaceState, moves []plan.Move) {
	for _, m := range moves {
		// Remove from source
		src := assignments[m.From]
		for i, s := range src {
			if s == m.ShardID {
				assignments[m.From] = append(src[:i], src[i+1:]...)
				break
			}
		}
		// Add to dest
		assignments[m.To] = append(assignments[m.To], m.ShardID)

		// Update namespace state
		delete(ns.ShardAssignments[m.From].AssignedShards, m.ShardID)
		ns.ShardAssignments[m.To].AssignedShards[m.ShardID] = &types.ShardAssignment{Status: types.AssignmentStatusREADY}
	}
}

func computeLoads(assignments map[string][]string, ns *store.NamespaceState) map[string]float64 {
	loads := make(map[string]float64, len(assignments))
	for executorID, shards := range assignments {
		for _, shardID := range shards {
			if stats, ok := ns.ShardStats[shardID]; ok {
				loads[executorID] += stats.SmoothedLoad
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

// loadHistoryRow is a single timestamped snapshot of per-shard load values.
type loadHistoryRow struct {
	Timestamp  time.Time
	ShardLoads map[string]float64
}

// loadCSVHistory reads a CSV file in the format:
//
//	<timestamp>, <shard_0_load>, <shard_1_load>, ...
//
// The first row is expected to be a data row (no header). Shard IDs are
// generated as "0", "1", "2", … matching the column index (after the timestamp
// column). The timestamp format is "2006-01-02 15:04:05".
//
// If the file has a header row (first column is not parseable as a timestamp),
// it is silently skipped.
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
