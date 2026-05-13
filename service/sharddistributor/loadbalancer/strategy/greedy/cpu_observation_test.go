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

func TestUpdateExecutorCPUObservation_Smoothing(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	state.SetSmoothingTau(300 * time.Second)

	// First heartbeat only stores the sample.
	_, ok := state.updateExecutorCPUObservation("exec-1", meta(10, now))
	require.False(t, ok)

	// Second heartbeat produces the first delta; no prior smoothed value exists,
	// so the raw busy-cores rate is returned directly.
	busyCores, ok := state.updateExecutorCPUObservation("exec-1", meta(25, now.Add(10*time.Second)))
	require.True(t, ok)
	require.InDelta(t, 1.5, busyCores, 1e-9)

	// Third heartbeat: raw rate jumps to 2.0 but the returned value should be
	// EWMA-smoothed (pulled back toward the previous 1.5).
	busyCores, ok = state.updateExecutorCPUObservation("exec-1", meta(45, now.Add(20*time.Second)))
	require.True(t, ok)
	require.Greater(t, busyCores, 1.5)
	require.Less(t, busyCores, 2.0)

	// Fourth heartbeat: raw rate drops back to 1.5; smoothed value should move
	// downward but stay above the raw rate.
	busyCores, ok = state.updateExecutorCPUObservation("exec-1", meta(60, now.Add(30*time.Second)))
	require.True(t, ok)
	require.Greater(t, busyCores, 1.5)
	require.Less(t, busyCores, 2.0)
}

func TestUpdateExecutorCPUObservation_MissingSampleResetsSmoothing(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	state.SetSmoothingTau(300 * time.Second)

	// Build up a smoothed value.
	state.updateExecutorCPUObservation("exec-1", meta(10, now))
	state.updateExecutorCPUObservation("exec-1", meta(25, now.Add(10*time.Second)))
	_, ok := state.updateExecutorCPUObservation("exec-1", meta(45, now.Add(20*time.Second)))
	require.True(t, ok)

	// Invalid metadata wipes both the raw sample and the smoothed state.
	_, ok = state.updateExecutorCPUObservation("exec-1", map[string]string{})
	require.False(t, ok)

	// Next valid sample starts a fresh sequence.
	_, ok = state.updateExecutorCPUObservation("exec-1", meta(60, now.Add(30*time.Second)))
	require.False(t, ok)

	// Second sample in the new sequence returns raw rate again (no smoothing history).
	busyCores, ok := state.updateExecutorCPUObservation("exec-1", meta(75, now.Add(40*time.Second)))
	require.True(t, ok)
	require.InDelta(t, 1.5, busyCores, 1e-9)
}

func TestUpdateExecutorCPUObservations_CleansUpSmoothedForRemovedExecutors(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	state.SetSmoothingTau(300 * time.Second)

	namespaceState := &store.NamespaceState{
		Executors: map[string]store.HeartbeatState{
			"exec-1": {Metadata: meta(10, now)},
		},
	}

	// First call stores the sample.
	state.updateExecutorCPUObservations(namespaceState)
	// Second call creates the smoothed entry.
	namespaceState.Executors["exec-1"] = store.HeartbeatState{Metadata: meta(25, now.Add(10*time.Second))}
	state.updateExecutorCPUObservations(namespaceState)
	require.Contains(t, state.smoothed, "exec-1")

	// After the executor disappears, the smoothed entry should be cleaned up.
	delete(namespaceState.Executors, "exec-1")
	state.updateExecutorCPUObservations(namespaceState)
	require.NotContains(t, state.smoothed, "exec-1")
}

func TestUpdateExecutorCPUObservation_RawWhenTauIsZero(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	// smoothingTau defaults to 0, which means raw mode.

	state.updateExecutorCPUObservation("exec-1", meta(10, now))
	busyCores, ok := state.updateExecutorCPUObservation("exec-1", meta(25, now.Add(10*time.Second)))
	require.True(t, ok)
	require.InDelta(t, 1.5, busyCores, 1e-9)

	// Even when the rate changes, raw mode returns the exact new rate.
	busyCores, ok = state.updateExecutorCPUObservation("exec-1", meta(50, now.Add(20*time.Second)))
	require.True(t, ok)
	require.InDelta(t, 2.5, busyCores, 1e-9)

	// No smoothed state should be accumulated in raw mode.
	require.NotContains(t, state.smoothed, "exec-1")
}

func TestUpdateExecutorCPUObservation_DuplicateSamplePreservesSmoothed(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	state := NewCPUObservationState()
	state.SetSmoothingTau(300 * time.Second)

	// First two samples build up a smoothed value.
	state.updateExecutorCPUObservation("exec-1", meta(10, now))
	state.updateExecutorCPUObservation("exec-1", meta(25, now.Add(10*time.Second)))
	state.updateExecutorCPUObservation("exec-1", meta(45, now.Add(20*time.Second)))

	smoothedBefore := state.smoothed["exec-1"].busyCores

	// Same sample again (rebalance ran before next heartbeat).
	busyCores, ok := state.updateExecutorCPUObservation("exec-1", meta(45, now.Add(20*time.Second)))
	require.True(t, ok)
	require.InDelta(t, smoothedBefore, busyCores, 1e-9)
	require.InDelta(t, smoothedBefore, state.smoothed["exec-1"].busyCores, 1e-9)
}
