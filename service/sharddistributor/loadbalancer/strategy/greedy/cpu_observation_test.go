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

	// First heartbeat only stores the sample.
	_, ok := state.updateExecutorCPUCostObservation("exec-1", meta(10, now), 10)
	require.False(t, ok)

	// Second heartbeat produces the first delta; no prior smoothed value exists,
	// so the raw CPU cost is returned directly.
	cost, ok := state.updateExecutorCPUCostObservation("exec-1", meta(25, now.Add(10*time.Second)), 10)
	require.True(t, ok)
	require.InDelta(t, 0.15, cost, 1e-9)

	// Third heartbeat: raw cost jumps to 0.2 but the returned value should be
	// EWMA-smoothed (pulled back toward the previous 0.15).
	cost, ok = state.updateExecutorCPUCostObservation("exec-1", meta(45, now.Add(20*time.Second)), 10)
	require.True(t, ok)
	require.Greater(t, cost, 0.15)
	require.Less(t, cost, 0.2)

	// Fourth heartbeat: raw cost drops back to 0.15; smoothed value should move
	// downward but stay above the raw rate.
	cost, ok = state.updateExecutorCPUCostObservation("exec-1", meta(60, now.Add(30*time.Second)), 10)
	require.True(t, ok)
	require.Greater(t, cost, 0.15)
	require.Less(t, cost, 0.2)
}

func TestUpdateExecutorCPUDiagnostic_SmoothsBusyCores(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	state.SetSmoothingTau(300 * time.Second)

	_, ok := state.updateExecutorCPUDiagnostic("exec-1", meta(10, now), 10)
	require.False(t, ok)

	diagnostic, ok := state.updateExecutorCPUDiagnostic("exec-1", meta(25, now.Add(10*time.Second)), 10)
	require.True(t, ok)
	require.InDelta(t, 1.5, diagnostic.busyCores, 1e-9)
	require.InDelta(t, 1.5, diagnostic.smoothedBusyCores, 1e-9)

	diagnostic, ok = state.updateExecutorCPUDiagnostic("exec-1", meta(45, now.Add(20*time.Second)), 10)
	require.True(t, ok)
	require.InDelta(t, 2.0, diagnostic.busyCores, 1e-9)
	require.Greater(t, diagnostic.smoothedBusyCores, 1.5)
	require.Less(t, diagnostic.smoothedBusyCores, 2.0)
}

func TestUpdateExecutorCPUCostObservation_MissingSamplePreservesSmoothing(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	state.SetSmoothingTau(300 * time.Second)

	// Build up a smoothed value.
	state.updateExecutorCPUCostObservation("exec-1", meta(10, now), 10)
	state.updateExecutorCPUCostObservation("exec-1", meta(25, now.Add(10*time.Second)), 10)
	smoothedBefore, ok := state.updateExecutorCPUCostObservation("exec-1", meta(45, now.Add(20*time.Second)), 10)
	require.True(t, ok)

	// Invalid metadata should not wipe the learned smoothed cost.
	cost, ok := state.updateExecutorCPUCostObservation("exec-1", map[string]string{}, 10)
	require.True(t, ok)
	require.InDelta(t, smoothedBefore, cost, 1e-9)

	// Once a fresh sample arrives again, smoothing continues from the preserved value.
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

	// First call stores the sample.
	state.updateExecutorCPUCostObservations(namespaceState, map[string]float64{"exec-1": 10})
	// Second call creates the smoothed entry.
	namespaceState.Executors["exec-1"] = store.HeartbeatState{Metadata: meta(25, now.Add(10*time.Second))}
	state.updateExecutorCPUCostObservations(namespaceState, map[string]float64{"exec-1": 10})
	require.Contains(t, state.smoothedCosts, "exec-1")
	require.Contains(t, state.samples, "exec-1")

	// A transiently missing executor should keep its learned state.
	delete(namespaceState.Executors, "exec-1")
	state.updateExecutorCPUCostObservations(namespaceState, map[string]float64{"exec-1": 10})
	require.Contains(t, state.smoothedCosts, "exec-1")
	require.Contains(t, state.samples, "exec-1")

	// When the executor returns, its next valid delta should continue from the
	// preserved state instead of reseeding from scratch.
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

	// A counter reset should keep the previous smoothed value rather than wiping it.
	cost, ok := state.updateExecutorCPUCostObservation("exec-1", meta(5, now.Add(30*time.Second)), 10)
	require.True(t, ok)
	require.InDelta(t, smoothedBefore, cost, 1e-9)

	// The next valid sample should re-anchor from the reset point and resume smoothing.
	cost, ok = state.updateExecutorCPUCostObservation("exec-1", meta(20, now.Add(40*time.Second)), 10)
	require.True(t, ok)
	require.Greater(t, cost, 0.15)
	require.Less(t, cost, 0.2)
}

func TestUpdateExecutorCPUCostObservation_RawWhenTauIsZero(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	// smoothingTau defaults to 0, which means raw mode.

	state.updateExecutorCPUCostObservation("exec-1", meta(10, now), 10)
	cost, ok := state.updateExecutorCPUCostObservation("exec-1", meta(25, now.Add(10*time.Second)), 10)
	require.True(t, ok)
	require.InDelta(t, 0.15, cost, 1e-9)

	// Even when the rate changes, raw mode returns the exact new rate.
	cost, ok = state.updateExecutorCPUCostObservation("exec-1", meta(50, now.Add(20*time.Second)), 10)
	require.True(t, ok)
	require.InDelta(t, 0.25, cost, 1e-9)

	// No smoothed state should be accumulated in raw mode.
	require.NotContains(t, state.smoothedCosts, "exec-1")
}

func TestUpdateExecutorCPUCostObservation_DuplicateSamplePreservesSmoothedCost(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	state.SetSmoothingTau(300 * time.Second)

	// First two samples build up a smoothed value.
	state.updateExecutorCPUCostObservation("exec-1", meta(10, now), 10)
	state.updateExecutorCPUCostObservation("exec-1", meta(25, now.Add(10*time.Second)), 10)
	state.updateExecutorCPUCostObservation("exec-1", meta(45, now.Add(20*time.Second)), 10)

	smoothedBefore := state.smoothedCosts["exec-1"].cost

	// Same sample again (rebalance ran before next heartbeat). A different
	// current load must not change the returned cost for a stale CPU sample.
	cost, ok := state.updateExecutorCPUCostObservation("exec-1", meta(45, now.Add(20*time.Second)), 100)
	require.True(t, ok)
	require.InDelta(t, smoothedBefore, cost, 1e-9)
	require.InDelta(t, smoothedBefore, state.smoothedCosts["exec-1"].cost, 1e-9)
}
