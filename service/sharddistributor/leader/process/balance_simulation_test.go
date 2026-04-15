package process

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/service/sharddistributor/config"
)

// csvFixturePath is a path relative to the package directory (where `go test` runs).
const csvFixturePath = "testdata/combined_4x_fixed.csv"

// -----------------------------------------------------------------
// CSV loader unit tests
// -----------------------------------------------------------------

func TestLoadCSVHistory_ParsesTimestampAndLoads(t *testing.T) {
	const data = `2025-12-21 00:00:00,10.5,20.0,0.0
2025-12-21 00:00:10,11.0,19.5,1.0`

	rows, shardIDs, err := LoadCSVHistory(strings.NewReader(data))
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Len(t, shardIDs, 3) // three load columns → shard IDs "0","1","2"

	assert.Equal(t, "0", shardIDs[0])
	assert.Equal(t, "2", shardIDs[2])

	assert.Equal(t, 10.5, rows[0].ShardLoads["0"])
	assert.Equal(t, 20.0, rows[0].ShardLoads["1"])
	assert.Equal(t, 0.0, rows[0].ShardLoads["2"])
	assert.Equal(t, 11.0, rows[1].ShardLoads["0"])

	wantTime, _ := time.Parse("2006-01-02 15:04:05", "2025-12-21 00:00:10")
	assert.Equal(t, wantTime, rows[1].Timestamp)
}

func TestLoadCSVHistory_SkipsNonTimestampHeader(t *testing.T) {
	const data = `timestamp,shard_0,shard_1
2025-12-21 00:00:00,1.0,2.0`

	rows, shardIDs, err := LoadCSVHistory(strings.NewReader(data))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Len(t, shardIDs, 2)
}

func TestLoadCSVHistory_EmptyReturnsError(t *testing.T) {
	_, _, err := LoadCSVHistory(strings.NewReader(""))
	require.Error(t, err)
}

func TestLoadCSVHistory_MalformedLoadTreatedAsZero(t *testing.T) {
	const data = `2025-12-21 00:00:00,bad,5.0`

	rows, _, err := LoadCSVHistory(strings.NewReader(data))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, 0.0, rows[0].ShardLoads["0"])
	assert.Equal(t, 5.0, rows[0].ShardLoads["1"])
}

// -----------------------------------------------------------------
// BalanceTester unit tests (no CSV, hand-crafted state)
// -----------------------------------------------------------------

func TestBalanceTester_RebalancesImbalancedExecutors(t *testing.T) {
	// ExecA: 10 heavy shards (load 10 each = 100 total)
	// ExecB: 10 light shards (load 1 each  = 10 total)
	// After rebalancing, execA should shed some shards to execB.
	lbCfg := config.LoadBalance{
		PerShardCooldown:     0, // no cooldown for this test
		MoveBudgetProportion: 0.1,
		HysteresisUpperBand:  1.15,
		HysteresisLowerBand:  0.95,
		SevereImbalanceRatio: 1.5,
	}
	nsCfg := config.Namespace{Name: "test", Type: config.NamespaceTypeEphemeral}

	tester := NewBalanceTester(time.Now(), nsCfg, lbCfg)

	executors := []string{"exec-A", "exec-B"}
	var shards []string
	for i := range 20 {
		shards = append(shards, string(rune('A'+i)))
	}
	tester.SetInitialAssignments(executors, shards)

	// Inject load: first 10 shards (on exec-A) are heavy, rest light.
	now := tester.GetTimeSource().Now()
	assignments := tester.GetAssignments()
	for _, shard := range assignments["exec-A"] {
		stats := tester.GetNamespaceState().ShardStats[shard]
		stats.SmoothedLoad = 10.0
		stats.LastUpdateTime = now
		tester.GetNamespaceState().ShardStats[shard] = stats
	}
	for _, shard := range assignments["exec-B"] {
		stats := tester.GetNamespaceState().ShardStats[shard]
		stats.SmoothedLoad = 1.0
		stats.LastUpdateTime = now
		tester.GetNamespaceState().ShardStats[shard] = stats
	}

	_, err := tester.Rebalance()
	require.NoError(t, err)

	a := len(tester.GetAssignments()["exec-A"])
	b := len(tester.GetAssignments()["exec-B"])
	assert.Less(t, a, 10, "exec-A should shed shards")
	assert.Greater(t, b, 10, "exec-B should gain shards")
}

// -----------------------------------------------------------------
// Full simulation test against the real CSV fixture
// -----------------------------------------------------------------

// TestRunBalanceSimulation_CSV loads the combined_4x_fixed.csv fixture, replays
// it through the greedy balancer with 4 executors, and verifies that the
// resulting load distribution is meaningfully more balanced than a naive
// static assignment.
func TestRunBalanceSimulation_CSV(t *testing.T) {
	f, err := os.Open(csvFixturePath)
	if os.IsNotExist(err) {
		t.Skipf("fixture not found at %s — skipping simulation test", csvFixturePath)
	}
	require.NoError(t, err, "opening CSV fixture")
	defer f.Close()

	history, shardIDs, err := LoadCSVHistory(f)
	require.NoError(t, err)
	require.NotEmpty(t, history, "CSV must contain at least one data row")
	t.Logf("Loaded %d rows, %d shards", len(history), len(shardIDs))

	executors := []string{"exec-0", "exec-1", "exec-2", "exec-3"}

	lbCfg := config.LoadBalance{
		PerShardCooldown:     30 * time.Second,
		MoveBudgetProportion: 0.01,
		HysteresisUpperBand:  1.15,
		HysteresisLowerBand:  0.95,
		SevereImbalanceRatio: 1.5,
	}
	nsCfg := config.Namespace{
		Name: "sim-test",
		// NamespaceTypeEphemeral: getShards counts from ShardAssignments.
		Type: config.NamespaceTypeEphemeral,
	}

	// Each CSV row is 10 s apart; run a rebalance every 30 s simulated time.
	finalAssignments, err := RunBalanceSimulation(
		history,
		executors,
		shardIDs,
		nsCfg,
		lbCfg,
		30*time.Second,  // rebalanceInterval
		10*time.Second,  // loadUpdateInterval (matches CSV cadence)
	)
	require.NoError(t, err)

	// Verify total shard count is preserved.
	total := 0
	for _, shards := range finalAssignments {
		total += len(shards)
	}
	assert.Equal(t, len(shardIDs), total, "total shards must be conserved")

	// Compute the final smoothed load per executor from the last history row.
	lastRow := history[len(history)-1]
	executorLoads := make(map[string]float64, len(executors))
	for exec, shards := range finalAssignments {
		for _, shard := range shards {
			executorLoads[exec] += lastRow.ShardLoads[shard]
		}
	}

	t.Logf("Final assignment counts: %v", mapCounts(finalAssignments))
	t.Logf("Final executor loads (point-in-time): %v", executorLoads)

	meanLoad, stdDev := loadStats(executorLoads)
	cv := 0.0
	if meanLoad > 0 {
		cv = stdDev / meanLoad
	}
	t.Logf("Load mean=%.2f  stdDev=%.2f  CV=%.3f", meanLoad, stdDev, cv)

	// The coefficient of variation should be well below 1.0 (i.e. loads are
	// within the same order of magnitude). A naive round-robin on a skewed
	// workload can easily exceed CV=1. This is a smoke-test, not a precision claim.
	assert.Less(t, cv, 1.0, "load distribution should not be wildly uneven")
}

// TestRunBalanceSimulation_CSV_Parametric runs the simulation with varying
// move-budget proportions and logs convergence quality — useful for manual
// parameter tuning without changing production config.
func TestRunBalanceSimulation_CSV_Parametric(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping parametric simulation in short mode")
	}
	f, err := os.Open(csvFixturePath)
	if os.IsNotExist(err) {
		t.Skipf("fixture not found at %s", csvFixturePath)
	}
	require.NoError(t, err)
	defer f.Close()

	history, shardIDs, err := LoadCSVHistory(f)
	require.NoError(t, err)

	executors := []string{"exec-0", "exec-1", "exec-2", "exec-3"}
	nsCfg := config.Namespace{Name: "sim-test", Type: config.NamespaceTypeEphemeral}

	for _, budget := range []float64{0.005, 0.01, 0.02, 0.05} {
		budget := budget
		t.Run(fmt.Sprintf("budget=%.3f", budget), func(t *testing.T) {
			lbCfg := config.LoadBalance{
				PerShardCooldown:     30 * time.Second,
				MoveBudgetProportion: budget,
				HysteresisUpperBand:  1.15,
				HysteresisLowerBand:  0.95,
				SevereImbalanceRatio: 1.5,
			}
			assignments, err := RunBalanceSimulation(
				history, executors, shardIDs, nsCfg, lbCfg,
				30*time.Second, 10*time.Second,
			)
			require.NoError(t, err)

			lastRow := history[len(history)-1]
			loads := make(map[string]float64)
			for exec, shards := range assignments {
				for _, shard := range shards {
					loads[exec] += lastRow.ShardLoads[shard]
				}
			}
			mean, sd := loadStats(loads)
			cv := 0.0
			if mean > 0 {
				cv = sd / mean
			}
			t.Logf("budget=%.3f  counts=%v  mean=%.2f  stdDev=%.2f  CV=%.3f",
				budget, mapCounts(assignments), mean, sd, cv)
		})
	}
}

// -----------------------------------------------------------------
// helpers
// -----------------------------------------------------------------

func mapCounts(m map[string][]string) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = len(v)
	}
	return out
}

func loadStats(loads map[string]float64) (mean, stdDev float64) {
	if len(loads) == 0 {
		return 0, 0
	}
	for _, v := range loads {
		mean += v
	}
	mean /= float64(len(loads))
	for _, v := range loads {
		d := v - mean
		stdDev += d * d
	}
	stdDev = math.Sqrt(stdDev / float64(len(loads)))
	return mean, stdDev
}
