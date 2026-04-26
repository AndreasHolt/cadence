package process

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/service/sharddistributor/capacity"
	"github.com/uber/cadence/service/sharddistributor/store"
)

func cpuMetadata(goMaxProcs int, processCPUSeconds float64, sampleTime time.Time) map[string]string {
	return capacity.HeartbeatMetadata(nil, capacity.HeartbeatMetadataOptions{
		GoMaxProcs:        goMaxProcs,
		ProcessCPUSeconds: processCPUSeconds,
		HasProcessCPU:     true,
		SampleUnixNanos:   sampleTime.UnixNano(),
	})
}

func TestComputeExecutorCPUObservation(t *testing.T) {
	tests := []struct {
		name     string
		previous executorCPUSample
		current  executorCPUSample
		want     executorCPUObservation
		wantOK   bool
	}{
		{
			name: "valid sample computes busy cores",
			previous: executorCPUSample{
				processCPUSeconds: 10,
				sampleTime:        time.Unix(100, 0),
			},
			current: executorCPUSample{
				processCPUSeconds: 16,
				sampleTime:        time.Unix(103, 0),
			},
			want:   executorCPUObservation{busyCores: 2},
			wantOK: true,
		},
		{
			name: "first timestamp equal to current is invalid",
			previous: executorCPUSample{
				processCPUSeconds: 10,
				sampleTime:        time.Unix(100, 0),
			},
			current: executorCPUSample{
				processCPUSeconds: 11,
				sampleTime:        time.Unix(100, 0),
			},
		},
		{
			name: "negative cpu delta is invalid",
			previous: executorCPUSample{
				processCPUSeconds: 10,
				sampleTime:        time.Unix(100, 0),
			},
			current: executorCPUSample{
				processCPUSeconds: 9,
				sampleTime:        time.Unix(101, 0),
			},
		},
		{
			name: "nan cpu delta is invalid",
			previous: executorCPUSample{
				processCPUSeconds: math.NaN(),
				sampleTime:        time.Unix(100, 0),
			},
			current: executorCPUSample{
				processCPUSeconds: 11,
				sampleTime:        time.Unix(101, 0),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := computeExecutorCPUObservation(tt.previous, tt.current)

			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestUpdateExecutorCPUObservation(t *testing.T) {
	processor := &namespaceProcessor{}

	firstSampleTime := time.Unix(100, 0)
	observation, ok := processor.updateExecutorCPUObservation("executor-a", cpuMetadata(4, 10, firstSampleTime))
	require.False(t, ok)
	require.Equal(t, executorCPUObservation{}, observation)

	secondSampleTime := firstSampleTime.Add(4 * time.Second)
	observation, ok = processor.updateExecutorCPUObservation("executor-a", cpuMetadata(4, 18, secondSampleTime))
	require.True(t, ok)
	require.Equal(t, executorCPUObservation{busyCores: 2}, observation)
}

func TestUpdateExecutorCPUObservationRejectsInvalidMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
	}{
		{
			name: "missing metadata",
		},
		{
			name: "runtime metadata without cpu sample",
			metadata: capacity.HeartbeatMetadata(nil, capacity.HeartbeatMetadataOptions{
				GoMaxProcs: 4,
			}),
		},
		{
			name: "invalid runtime metadata",
			metadata: map[string]string{
				capacity.RuntimeMetadataKey: "not-json",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := &namespaceProcessor{}
			processor.executorCPUSamples = map[string]executorCPUSample{
				"executor-a": {
					processCPUSeconds: 10,
					sampleTime:        time.Unix(100, 0),
				},
			}

			observation, ok := processor.updateExecutorCPUObservation("executor-a", tt.metadata)

			require.False(t, ok)
			require.Equal(t, executorCPUObservation{}, observation)
			require.NotContains(t, processor.executorCPUSamples, "executor-a")
		})
	}
}

func TestUpdateExecutorCPUObservationsPrunesMissingExecutors(t *testing.T) {
	processor := &namespaceProcessor{}
	firstSampleTime := time.Unix(100, 0)
	secondSampleTime := firstSampleTime.Add(time.Second)

	processor.updateExecutorCPUObservations(&store.NamespaceState{
		Executors: map[string]store.HeartbeatState{
			"executor-a": {Metadata: cpuMetadata(4, 10, firstSampleTime)},
		},
	})
	processor.updateExecutorCPUObservations(&store.NamespaceState{
		Executors: map[string]store.HeartbeatState{
			"executor-a": {Metadata: cpuMetadata(4, 12, secondSampleTime)},
		},
	})
	require.Contains(t, processor.executorCPUSamples, "executor-a")
	require.Contains(t, processor.executorCPUObservations, "executor-a")

	processor.updateExecutorCPUObservations(&store.NamespaceState{
		Executors: map[string]store.HeartbeatState{},
	})
	require.NotContains(t, processor.executorCPUSamples, "executor-a")
	require.NotContains(t, processor.executorCPUObservations, "executor-a")
}
