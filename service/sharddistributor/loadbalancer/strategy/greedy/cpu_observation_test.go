package greedy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/service/sharddistributor/capacity"
	"github.com/uber/cadence/service/sharddistributor/store"
)

func TestComputeExecutorCPUObservation(t *testing.T) {
	now := time.Unix(100, 0).UTC()

	tests := []struct {
		name           string
		previousSample executorCPUSample
		currentSample  executorCPUSample
		wantBusyCores  float64
		wantOK         bool
	}{
		{
			name: "valid",
			previousSample: executorCPUSample{
				processCPUSeconds: 10,
				sampleTime:        now,
			},
			currentSample: executorCPUSample{
				processCPUSeconds: 25,
				sampleTime:        now.Add(10 * time.Second),
			},
			wantBusyCores: 1.5,
			wantOK:        true,
		},
		{
			name: "cpu counter reset",
			previousSample: executorCPUSample{
				processCPUSeconds: 10,
				sampleTime:        now,
			},
			currentSample: executorCPUSample{
				processCPUSeconds: 9,
				sampleTime:        now.Add(10 * time.Second),
			},
			wantOK: false,
		},
		{
			name: "non increasing time",
			previousSample: executorCPUSample{
				processCPUSeconds: 10,
				sampleTime:        now,
			},
			currentSample: executorCPUSample{
				processCPUSeconds: 11,
				sampleTime:        now,
			},
			wantOK: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			busyCores, ok := computeExecutorCPUObservation(test.previousSample, test.currentSample)
			require.Equal(t, test.wantOK, ok)
			if test.wantOK {
				require.Equal(t, test.wantBusyCores, busyCores)
			}
		})
	}
}

func meta(cpuSeconds float64, sampleTime time.Time) map[string]string {
	return capacity.HeartbeatMetadataWithOptions(nil, capacity.HeartbeatMetadataOptions{
		GoMaxProcs:        4,
		ProcessCPUSeconds: cpuSeconds,
		HasProcessCPU:     true,
		SampleTime:        sampleTime,
	})
}

func TestUpdateExecutorCPUCostObservation_Smoothing(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	state.SetSmoothingTau(300 * time.Second)

	_, ok := state.updateExecutorCPUCostObservation("exec-1", meta(10, now), 10)
	require.False(t, ok)

	cost, ok := state.updateExecutorCPUCostObservation("exec-1", meta(25, now.Add(10*time.Second)), 10)
	require.True(t, ok)
	require.InDelta(t, 0.15, cost, 1e-9)

	cost, ok = state.updateExecutorCPUCostObservation("exec-1", meta(45, now.Add(20*time.Second)), 10)
	require.True(t, ok)
	require.Greater(t, cost, 0.15)
	require.Less(t, cost, 0.2)

	cost, ok = state.updateExecutorCPUCostObservation("exec-1", meta(60, now.Add(30*time.Second)), 10)
	require.True(t, ok)
	require.Greater(t, cost, 0.15)
	require.Less(t, cost, 0.2)
}

func TestUpdateExecutorCPUCostObservation_MissingSamplePreservesSmoothing(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	state.SetSmoothingTau(300 * time.Second)

	state.updateExecutorCPUCostObservation("exec-1", meta(10, now), 10)
	state.updateExecutorCPUCostObservation("exec-1", meta(25, now.Add(10*time.Second)), 10)
	smoothedBefore, ok := state.updateExecutorCPUCostObservation("exec-1", meta(45, now.Add(20*time.Second)), 10)
	require.True(t, ok)

	cost, ok := state.updateExecutorCPUCostObservation("exec-1", map[string]string{}, 10)
	require.True(t, ok)
	require.InDelta(t, smoothedBefore, cost, 1e-9)

	cost, ok = state.updateExecutorCPUCostObservation("exec-1", meta(60, now.Add(30*time.Second)), 10)
	require.True(t, ok)
	require.Greater(t, cost, 0.15)
	require.Less(t, cost, smoothedBefore)

	cost, ok = state.updateExecutorCPUCostObservation("exec-1", meta(80, now.Add(40*time.Second)), 10)
	require.True(t, ok)
	require.Greater(t, cost, 0.15)
	require.Less(t, cost, 0.2)
}

func TestUpdateExecutorCPUCostObservations_PreservesSmoothedForTransientlyMissingExecutors(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	state.SetSmoothingTau(300 * time.Second)

	namespaceState := &store.NamespaceState{
		Executors: map[string]store.HeartbeatState{
			"exec-1": {Metadata: meta(10, now)},
		},
	}

	state.updateExecutorCPUCostObservations(namespaceState, map[string]float64{"exec-1": 10})
	namespaceState.Executors["exec-1"] = store.HeartbeatState{Metadata: meta(25, now.Add(10*time.Second))}
	state.updateExecutorCPUCostObservations(namespaceState, map[string]float64{"exec-1": 10})
	require.Contains(t, state.smoothedCosts, "exec-1")
	require.Contains(t, state.samples, "exec-1")

	delete(namespaceState.Executors, "exec-1")
	state.updateExecutorCPUCostObservations(namespaceState, map[string]float64{"exec-1": 10})
	require.Contains(t, state.smoothedCosts, "exec-1")
	require.Contains(t, state.samples, "exec-1")

	namespaceState.Executors["exec-1"] = store.HeartbeatState{Metadata: meta(40, now.Add(20*time.Second))}
	costs := state.updateExecutorCPUCostObservations(namespaceState, map[string]float64{"exec-1": 10})
	require.Contains(t, costs, "exec-1")
	require.InDelta(t, 0.15, costs["exec-1"], 1e-9)

	namespaceState.Executors["exec-1"] = store.HeartbeatState{Metadata: meta(60, now.Add(30*time.Second))}
	costs = state.updateExecutorCPUCostObservations(namespaceState, map[string]float64{"exec-1": 10})
	require.Contains(t, costs, "exec-1")
	require.Greater(t, costs["exec-1"], 0.15)
	require.Less(t, costs["exec-1"], 0.2)
}

func TestUpdateExecutorCPUCostObservation_InvalidDeltaPreservesSmoothing(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	state.SetSmoothingTau(300 * time.Second)

	state.updateExecutorCPUCostObservation("exec-1", meta(10, now), 10)
	state.updateExecutorCPUCostObservation("exec-1", meta(25, now.Add(10*time.Second)), 10)
	smoothedBefore, ok := state.updateExecutorCPUCostObservation("exec-1", meta(45, now.Add(20*time.Second)), 10)
	require.True(t, ok)

	cost, ok := state.updateExecutorCPUCostObservation("exec-1", meta(5, now.Add(30*time.Second)), 10)
	require.True(t, ok)
	require.InDelta(t, smoothedBefore, cost, 1e-9)

	cost, ok = state.updateExecutorCPUCostObservation("exec-1", meta(20, now.Add(40*time.Second)), 10)
	require.True(t, ok)
	require.Greater(t, cost, 0.15)
	require.Less(t, cost, 0.2)
}

func TestUpdateExecutorCPUCostObservation_RawWhenTauIsZero(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()

	state.updateExecutorCPUCostObservation("exec-1", meta(10, now), 10)
	cost, ok := state.updateExecutorCPUCostObservation("exec-1", meta(25, now.Add(10*time.Second)), 10)
	require.True(t, ok)
	require.InDelta(t, 0.15, cost, 1e-9)

	cost, ok = state.updateExecutorCPUCostObservation("exec-1", meta(50, now.Add(20*time.Second)), 10)
	require.True(t, ok)
	require.InDelta(t, 0.25, cost, 1e-9)

	require.NotContains(t, state.smoothedCosts, "exec-1")
}

func TestUpdateExecutorCPUCostObservation_DuplicateSamplePreservesSmoothedCost(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	state.SetSmoothingTau(300 * time.Second)

	state.updateExecutorCPUCostObservation("exec-1", meta(10, now), 10)
	state.updateExecutorCPUCostObservation("exec-1", meta(25, now.Add(10*time.Second)), 10)
	state.updateExecutorCPUCostObservation("exec-1", meta(45, now.Add(20*time.Second)), 10)

	smoothedBefore := state.smoothedCosts["exec-1"].cost

	cost, ok := state.updateExecutorCPUCostObservation("exec-1", meta(45, now.Add(20*time.Second)), 100)
	require.True(t, ok)
	require.InDelta(t, smoothedBefore, cost, 1e-9)
	require.InDelta(t, smoothedBefore, state.smoothedCosts["exec-1"].cost, 1e-9)
}
