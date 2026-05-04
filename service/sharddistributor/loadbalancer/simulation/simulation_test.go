package simulation

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadCSVHistory(t *testing.T) {
	const data = `timestamp,shard_0,shard_1
2025-12-21 00:00:00,10.5,20
2025-12-21 00:00:10,11,bad`

	rows, shardIDs, err := LoadCSVHistory(strings.NewReader(data), 0)
	require.NoError(t, err)
	require.Equal(t, []string{"0", "1"}, shardIDs)
	require.Len(t, rows, 2)
	require.Equal(t, 10.5, rows[0].ShardLoads["0"])
	require.Equal(t, 0.0, rows[1].ShardLoads["1"])
}

func TestRunRebalancesDeterministically(t *testing.T) {
	start := time.Date(2025, 12, 21, 0, 0, 0, 0, time.UTC)
	history := []LoadHistoryRow{
		{
			Timestamp: start,
			ShardLoads: map[string]float64{
				"0": 100,
				"1": 100,
				"2": 1,
				"3": 1,
			},
		},
		{
			Timestamp: start.Add(10 * time.Second),
			ShardLoads: map[string]float64{
				"0": 100,
				"1": 100,
				"2": 1,
				"3": 1,
			},
		},
	}

	result, err := Run(history, []string{"0", "2", "1", "3"}, Config{
		ExecutorCount:        2,
		PerShardCooldown:     time.Second,
		MoveBudgetProportion: 0.5,
		HysteresisUpperBand:  1.05,
		HysteresisLowerBand:  0.95,
		SevereImbalanceRatio: 1.1,
	})
	require.NoError(t, err)
	require.Equal(t, 4, result.Shards)
	require.Equal(t, 2, result.Executors)
	require.Positive(t, result.Moves)
	require.Less(t, result.FinalSmoothedCV, 1.0)
	require.Less(t, result.AverageSmoothedCV, 1.0)
	require.Less(t, result.WorstSmoothedCV, 1.0)
}
