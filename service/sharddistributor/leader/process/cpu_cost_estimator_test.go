package process

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/service/sharddistributor/store"
)

func TestExecutorCPUCostStateEstimate(t *testing.T) {
	state := executorCPUCostState{}
	for _, load := range []float64{10, 20, 30, 40, 50} {
		state = state.withObservation(load, executorCPUObservation{
			busyCores:       0.2 + 0.1*load,
			intervalSeconds: 3,
		})
	}

	estimate, ok := state.estimate()

	require.True(t, ok)
	require.InDelta(t, 0.2, estimate.baselineBusyCores, 1e-9)
	require.InDelta(t, 0.1, estimate.cpuCostPerLoadUnit, 1e-9)
	require.GreaterOrEqual(t, estimate.sampleWeight, minExecutorCPUCostSampleWeight)
}

func TestExecutorCPUCostStateEstimateRequiresEnoughWeight(t *testing.T) {
	state := executorCPUCostState{}
	state = state.withObservation(10, executorCPUObservation{
		busyCores:       1,
		intervalSeconds: 1,
	})

	estimate, ok := state.estimate()

	require.False(t, ok)
	require.Equal(t, executorCPUCostEstimate{}, estimate)
}

func TestExecutorCPUCostStateEstimateRequiresLoadVariance(t *testing.T) {
	state := executorCPUCostState{}
	for range 5 {
		state = state.withObservation(10, executorCPUObservation{
			busyCores:       1,
			intervalSeconds: 3,
		})
	}

	estimate, ok := state.estimate()

	require.False(t, ok)
	require.Equal(t, executorCPUCostEstimate{}, estimate)
}

func TestUpdateExecutorCPUCostEstimateRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name        string
		load        float64
		observation executorCPUObservation
	}{
		{
			name: "negative load",
			load: -1,
			observation: executorCPUObservation{
				busyCores:       1,
				intervalSeconds: 1,
			},
		},
		{
			name: "nan busy cores",
			load: 1,
			observation: executorCPUObservation{
				busyCores:       math.NaN(),
				intervalSeconds: 1,
			},
		},
		{
			name: "zero interval",
			load: 1,
			observation: executorCPUObservation{
				busyCores: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := &namespaceProcessor{
				executorCPUCostStates: map[string]executorCPUCostState{
					"executor-a": {sampleWeight: 10},
				},
			}

			estimate, ok := processor.updateExecutorCPUCostEstimate("executor-a", tt.load, tt.observation)

			require.False(t, ok)
			require.Equal(t, executorCPUCostEstimate{}, estimate)
			require.NotContains(t, processor.executorCPUCostStates, "executor-a")
		})
	}
}

func TestUpdateExecutorCPUCostEstimatesPrunesMissingExecutors(t *testing.T) {
	processor := &namespaceProcessor{}
	loads := map[string]float64{"executor-a": 10}
	observations := map[string]executorCPUObservation{
		"executor-a": {
			busyCores:       1,
			intervalSeconds: 3,
		},
	}
	namespaceState := &store.NamespaceState{
		Executors: map[string]store.HeartbeatState{
			"executor-a": {Metadata: cpuMetadata(4, 10, time.Unix(100, 0))},
		},
	}

	processor.updateExecutorCPUCostEstimates(loads, observations, namespaceState)
	require.Contains(t, processor.executorCPUCostStates, "executor-a")

	processor.updateExecutorCPUCostEstimates(loads, observations, &store.NamespaceState{
		Executors: map[string]store.HeartbeatState{},
	})

	require.NotContains(t, processor.executorCPUCostStates, "executor-a")
	require.NotContains(t, processor.executorCPUCostEstimates, "executor-a")
}

func TestUpdateExecutorCPUCostEstimatesDropsInvalidCPUMetadata(t *testing.T) {
	processor := &namespaceProcessor{
		executorCPUCostStates: map[string]executorCPUCostState{
			"executor-a": {sampleWeight: 10},
		},
		executorCPUCostEstimates: map[string]executorCPUCostEstimate{
			"executor-a": {cpuCostPerLoadUnit: 0.1},
		},
	}

	processor.updateExecutorCPUCostEstimates(nil, nil, &store.NamespaceState{
		Executors: map[string]store.HeartbeatState{
			"executor-a": {},
		},
	})

	require.NotContains(t, processor.executorCPUCostStates, "executor-a")
	require.NotContains(t, processor.executorCPUCostEstimates, "executor-a")
}

func TestUpdateExecutorCPUCostEstimatesKeepsStateWithoutNewObservation(t *testing.T) {
	processor := &namespaceProcessor{
		executorCPUCostStates: map[string]executorCPUCostState{
			"executor-a": {sampleWeight: 10},
		},
	}

	processor.updateExecutorCPUCostEstimates(nil, nil, &store.NamespaceState{
		Executors: map[string]store.HeartbeatState{
			"executor-a": {Metadata: cpuMetadata(4, 10, time.Unix(100, 0))},
		},
	})

	require.Contains(t, processor.executorCPUCostStates, "executor-a")
}
