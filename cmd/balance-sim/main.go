// balance-sim is a command-line tool for replaying real shard-load history
// through the Cadence shard-distributor greedy load-balance algorithm.
//
// Usage:
//
//	balance-sim -csv <file> [flags]
//
// Flags:
//
//	-csv string             Path to input CSV file (required).
//	                        Format: timestamp, shard_0_load, shard_1_load, ...
//	                        (no header row; first parseable-timestamp row wins).
//	-executors int          Number of simulated executors (default 4).
//	-out string             Output directory for metrics.csv (default ".").
//	-rebalance-interval     Simulated time between rebalance passes (default 30s).
//	-load-interval          Simulated time between CSV row advances (default 10s).
//	-move-budget float      Fraction of shards that may move per pass (default 0.01).
//	-cooldown duration      Per-shard move cooldown (default 1m).
//	-upper-band float       Hysteresis upper band multiplier (default 1.15).
//	-lower-band float       Hysteresis lower band multiplier (default 0.95).
//	-severe-ratio float     Severe-imbalance escape-hatch ratio (default 1.5).
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
	"time"

	"github.com/uber/cadence/service/sharddistributor/config"
	"github.com/uber/cadence/service/sharddistributor/leader/process"
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
		useSwap           bool
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
	flag.BoolVar(&useSwap, "use-swap", false, "Use swap moves")
	flag.Parse()

	if csvPath == "" {
		flag.Usage()
		return fmt.Errorf("-csv is required")
	}
	if numExecutors < 1 {
		return fmt.Errorf("-executors must be >= 1")
	}

	// ── Load history ─────────────────────────────────────────────────────────
	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("open CSV: %w", err)
	}
	defer f.Close()

	history, shardIDs, err := process.LoadCSVHistory(f)
	if err != nil {
		return fmt.Errorf("parse CSV: %w", err)
	}
	fmt.Printf("Loaded %d rows, %d shards from %s\n", len(history), len(shardIDs), csvPath)

	// ── Build executor list ───────────────────────────────────────────────────
	executors := make([]string, numExecutors)
	for i := range executors {
		executors[i] = fmt.Sprintf("exec-%d", i)
	}

	// ── Configure tester ─────────────────────────────────────────────────────
	lbCfg := config.LoadBalance{
		PerShardCooldown:     cooldown,
		MoveBudgetProportion: moveBudget,
		HysteresisUpperBand:  upperBand,
		HysteresisLowerBand:  lowerBand,
		SevereImbalanceRatio: severeRatio,
		UseSwap:              useSwap,
	}
	nsCfg := config.Namespace{
		Name: "balance-sim",
		Type: config.NamespaceTypeEphemeral,
	}

	tester := process.NewBalanceTester(history[0].Timestamp, nsCfg, lbCfg)
	tester.SetInitialAssignments(executors, shardIDs)

	// ── Open output file ──────────────────────────────────────────────────────
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

	// ── Simulation loop ───────────────────────────────────────────────────────
	currentHistoryIdx := 0
	nextLoadUpdate := history[0].Timestamp.Add(loadInterval)
	tickCount := 0

	for {
		ts := tester.GetTimeSource()
		currentTime := ts.Now()

		// Advance to the next history row when due.
		if currentTime.After(nextLoadUpdate) && currentHistoryIdx < len(history)-1 {
			currentHistoryIdx++
			tester.ApplyLoad(history[currentHistoryIdx])
			nextLoadUpdate = nextLoadUpdate.Add(loadInterval)
		}

		moves, err := tester.Rebalance()
		if err != nil {
			return fmt.Errorf("rebalance at tick %d: %w", tickCount, err)
		}

		// Compute and record metrics.
		loadsSmooth := tester.ComputeLoads()

		loadsRaw := make(map[string]float64)
		assignments := tester.GetAssignments()
		curRawLoads := history[currentHistoryIdx].ShardLoads
		for execID, shards := range assignments {
			var rawSum float64
			for _, shardID := range shards {
				rawSum += curRawLoads[shardID]
			}
			loadsRaw[execID] = rawSum
		}

		simTime := ts.Now().Format("2006-01-02 15:04:05")
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
		if err := writeRow(wMoves, float64(moves)); err != nil {
			return fmt.Errorf("write moves: %w", err)
		}

		tickCount++

		// Terminate after all history is consumed and one final rebalance is done.
		if currentHistoryIdx >= len(history)-1 && currentTime.After(nextLoadUpdate) {
			break
		}

		ts.Advance(rebalanceInterval)
	}

	for _, cleanup := range []func() error{closeSmoothMM, closeRawMM, closeSmoothCV, closeRawCV, closeMoves} {
		if err := cleanup(); err != nil {
			return fmt.Errorf("close csv: %w", err)
		}
	}

	fmt.Printf("Wrote %d rows to output directory %s\n", tickCount, outDir)
	return nil
}

// stats returns max, mean, standard deviation, and coefficient of variation
// for the given executor load map.
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
