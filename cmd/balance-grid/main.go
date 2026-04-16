package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/uber/cadence/service/sharddistributor/config"
	"github.com/uber/cadence/service/sharddistributor/leader/process"
)

type combo struct {
	cooldown    time.Duration
	moveBudget  float64
	upperBand   float64
	lowerBand   float64
	severeRatio float64
	useSwap     bool
}

func (c combo) ToRow() []string {
	return []string{
		c.cooldown.String(),
		strconv.FormatFloat(c.moveBudget, 'f', 4, 64),
		strconv.FormatFloat(c.upperBand, 'f', 4, 64),
		strconv.FormatFloat(c.lowerBand, 'f', 4, 64),
		strconv.FormatFloat(c.severeRatio, 'f', 4, 64),
		strconv.FormatBool(c.useSwap),
	}
}

type result struct {
	cb              combo
	totalMoves      int
	avgCVSmooth     float64
	worstCVSmooth   float64
	avgCVReported   float64
	worstCVReported float64
	avgMMSmooth     float64
	worstMMSmooth   float64
	avgMMReported   float64
	worstMMReported float64
}

func (r result) ToRow() []string {
	row := r.cb.ToRow()
	metrics := []string{
		strconv.Itoa(r.totalMoves),
		strconv.FormatFloat(r.avgCVSmooth, 'f', 6, 64),
		strconv.FormatFloat(r.worstCVSmooth, 'f', 6, 64),
		strconv.FormatFloat(r.avgCVReported, 'f', 6, 64),
		strconv.FormatFloat(r.worstCVReported, 'f', 6, 64),
		strconv.FormatFloat(r.avgMMSmooth, 'f', 6, 64),
		strconv.FormatFloat(r.worstMMSmooth, 'f', 6, 64),
		strconv.FormatFloat(r.avgMMReported, 'f', 6, 64),
		strconv.FormatFloat(r.worstMMReported, 'f', 6, 64),
	}
	return append(row, metrics...)
}

func main() {
	var (
		csvPath string
		outPath string
		execs   int
	)
	flag.StringVar(&csvPath, "csv", "", "Path to load history CSV (required)")
	flag.StringVar(&outPath, "out", "grid_results.csv", "Output CSV path")
	flag.IntVar(&execs, "executors", 4, "Number of simulated executors")
	flag.Parse()

	if csvPath == "" {
		fmt.Println("Missing required flag -csv")
		os.Exit(1)
	}

	executors := make([]string, execs)
	for i := 0; i < execs; i++ {
		executors[i] = fmt.Sprintf("exec-%d", i)
	}

	f, err := os.Open(csvPath)
	if err != nil {
		fmt.Printf("failed to open csv: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	fmt.Printf("Loading history from %s...\n", csvPath)
	history, shards, err := process.LoadCSVHistory(f)
	if err != nil {
		fmt.Printf("failed to parse csv: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d rows for %d shards.\n", len(history), len(shards))

	cooldowns := []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second}
	moveBudgets := []float64{0.005, 0.01, 0.02}
	upperBands := []float64{1.05, 1.15, 1.20, 1.5}
	lowerBands := []float64{0.6, 0.8, 0.90, 0.95}
	severeRatios := []float64{1.2, 1.3, 1.5, 2.0}
	useSwaps := []bool{false, true}

	var combos []combo
	for _, cooldown := range cooldowns {
		for _, moveBudget := range moveBudgets {
			for _, upperBand := range upperBands {
				for _, lowerBand := range lowerBands {
					for _, severeRatio := range severeRatios {
						for _, useSwap := range useSwaps {
							combos = append(combos, combo{
								cooldown:    cooldown,
								moveBudget:  moveBudget,
								upperBand:   upperBand,
								lowerBand:   lowerBand,
								severeRatio: severeRatio,
								useSwap:     useSwap,
							})
						}
					}
				}
			}
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
				res, err := runGridSimulation(cb, history, shards, executors)
				if err != nil {
					fmt.Printf("Simulation failed for combo %v: %v\n", cb, err)
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
		fmt.Printf("failed to create output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	w := csv.NewWriter(outFile)
	header := []string{
		"cooldown", "move_budget", "upper_band", "lower_band", "severe_ratio", "use_swap",
		"total_moves", "avg_cv_smooth", "worst_cv_smooth", "avg_cv_reported", "worst_cv_reported",
		"avg_mm_smooth", "worst_mm_smooth", "avg_mm_reported", "worst_mm_reported",
	}
	w.Write(header)

	count := 0
	for res := range results {
		w.Write(res.ToRow())
		count++
		if count%50 == 0 {
			fmt.Printf("Completed %d / %d runs\n", count, len(combos))
		}
	}
	w.Flush()

	fmt.Printf("Grid search complete. Results saved to %s\n", outPath)
}

func runGridSimulation(
	cb combo,
	history []process.LoadHistoryRow,
	shards []string,
	executors []string,
) (result, error) {

	nsCfg := config.Namespace{
		Name: "grid-sim",
		Type: config.NamespaceTypeEphemeral,
	}
	lbCfg := config.LoadBalance{
		PerShardCooldown:     cb.cooldown,
		MoveBudgetProportion: cb.moveBudget,
		HysteresisUpperBand:  cb.upperBand,
		HysteresisLowerBand:  cb.lowerBand,
		SevereImbalanceRatio: cb.severeRatio,
		UseSwap:              cb.useSwap,
	}

	tester := process.NewBalanceTester(history[0].Timestamp, nsCfg, lbCfg)
	tester.SetInitialAssignments(executors, shards)

	rebalanceInterval := 2 * time.Second
	loadInterval := 10 * time.Second
	skipDuration := 1 * time.Minute

	initialTime := history[0].Timestamp
	nextLoadUpdate := initialTime.Add(loadInterval)
	currentHistoryIdx := 0

	var (
		totalMoves                int
		count                     int
		sumCVSmooth, sumCVRaw     float64
		sumMMSmooth, sumMMRaw     float64
		worstCVSmooth, worstCVRaw float64
		worstMMSmooth, worstMMRaw float64
	)

	ts := tester.GetTimeSource()
	for {
		currentTime := ts.Now()

		if currentTime.After(nextLoadUpdate) && currentHistoryIdx < len(history)-1 {
			currentHistoryIdx++
			tester.ApplyLoad(history[currentHistoryIdx])
			nextLoadUpdate = nextLoadUpdate.Add(loadInterval)
		}

		moves, err := tester.Rebalance()
		if err != nil {
			return result{}, err
		}
		totalMoves += moves

		smoothedLoadsMap := tester.ComputeLoads()
		row := history[currentHistoryIdx]

		var smoothedLoads, rawLoads []float64
		assignments := tester.GetAssignments()
		for _, exec := range executors {
			smoothedLoads = append(smoothedLoads, smoothedLoadsMap[exec])

			rLoad := 0.0
			for _, shard := range assignments[exec] {
				rLoad += row.ShardLoads[shard]
			}
			rawLoads = append(rawLoads, rLoad)
		}

		cvSmooth := coefficientOfVariation(smoothedLoads)
		cvRaw := coefficientOfVariation(rawLoads)
		mmSmooth := maxOverMean(smoothedLoads)
		mmRaw := maxOverMean(rawLoads)

		count++
		sumCVSmooth += cvSmooth
		sumCVRaw += cvRaw
		sumMMSmooth += mmSmooth
		sumMMRaw += mmRaw

		if currentTime.Sub(initialTime) >= skipDuration {
			if cvSmooth > worstCVSmooth {
				worstCVSmooth = cvSmooth
			}
			if cvRaw > worstCVRaw {
				worstCVRaw = cvRaw
			}
			if mmSmooth > worstMMSmooth {
				worstMMSmooth = mmSmooth
			}
			if mmRaw > worstMMRaw {
				worstMMRaw = mmRaw
			}
		}

		if currentHistoryIdx >= len(history)-1 && currentTime.After(nextLoadUpdate) {
			break
		}

		ts.Advance(rebalanceInterval)
	}

	res := result{
		cb:         cb,
		totalMoves: totalMoves,
	}
	if count > 0 {
		res.avgCVSmooth = sumCVSmooth / float64(count)
		res.avgCVReported = sumCVRaw / float64(count)
		res.avgMMSmooth = sumMMSmooth / float64(count)
		res.avgMMReported = sumMMRaw / float64(count)
	}
	res.worstCVSmooth = worstCVSmooth
	res.worstCVReported = worstCVRaw
	res.worstMMSmooth = worstMMSmooth
	res.worstMMReported = worstMMRaw

	return res, nil
}

func coefficientOfVariation(loads []float64) float64 {
	mean := 0.0
	for _, l := range loads {
		mean += l
	}
	if len(loads) > 0 {
		mean /= float64(len(loads))
	}
	if mean == 0 {
		return 0
	}
	variance := 0.0
	for _, l := range loads {
		variance += math.Pow(l-mean, 2)
	}
	stdDev := math.Sqrt(variance / float64(len(loads)))
	return stdDev / mean
}

func maxOverMean(loads []float64) float64 {
	max := 0.0
	mean := 0.0
	for _, l := range loads {
		if l > max {
			max = l
		}
		mean += l
	}
	if len(loads) > 0 {
		mean /= float64(len(loads))
	}
	if mean == 0 {
		return 0
	}
	return max / mean
}
