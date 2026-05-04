package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/uber/cadence/service/sharddistributor/loadbalancer/simulation"
)

func main() {
	var (
		csvPath              = flag.String("csv", "", "path to load history CSV")
		executors            = flag.Int("executors", 4, "number of simulated executors")
		maxRows              = flag.Int("max-rows", 0, "maximum CSV rows to replay; 0 means all rows")
		perShardCooldown     = flag.Duration("per-shard-cooldown", 30*time.Second, "per-shard move cooldown")
		moveBudgetProportion = flag.Float64("move-budget-proportion", 0.01, "fraction of total shards movable per row")
		hysteresisUpperBand  = flag.Float64("hysteresis-upper-band", 1.15, "source threshold relative to mean load")
		hysteresisLowerBand  = flag.Float64("hysteresis-lower-band", 0.90, "destination threshold relative to mean load")
		severeImbalanceRatio = flag.Float64("severe-imbalance-ratio", 1.3, "max/mean ratio that relaxes destination selection")
	)
	flag.Parse()

	if *csvPath == "" {
		fmt.Fprintln(os.Stderr, "-csv is required")
		os.Exit(2)
	}

	file, err := os.Open(*csvPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open csv: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	history, shardIDs, err := simulation.LoadCSVHistory(file, *maxRows)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load csv: %v\n", err)
		os.Exit(1)
	}

	result, err := simulation.Run(history, shardIDs, simulation.Config{
		ExecutorCount:        *executors,
		PerShardCooldown:     *perShardCooldown,
		MoveBudgetProportion: *moveBudgetProportion,
		HysteresisUpperBand:  *hysteresisUpperBand,
		HysteresisLowerBand:  *hysteresisLowerBand,
		SevereImbalanceRatio: *severeImbalanceRatio,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "run replay: %v\n", err)
		os.Exit(1)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintf(os.Stderr, "encode result: %v\n", err)
		os.Exit(1)
	}
}
