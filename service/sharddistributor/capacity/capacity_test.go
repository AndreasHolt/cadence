package capacity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHeartbeatMetadataWithOptionsIncludesProcessCPUSample(t *testing.T) {
	sampleTime := time.Unix(100, 123).UTC()

	metadata := HeartbeatMetadataWithOptions(map[string]string{"existing": "value"}, HeartbeatMetadataOptions{
		GoMaxProcs:        4,
		ProcessCPUSeconds: 12.5,
		HasProcessCPU:     true,
		SampleTime:        sampleTime,
	})

	require.Equal(t, "value", metadata["existing"])
	require.Equal(t, "4", metadata[GoMaxProcsMetadataKey])

	cpuSeconds, parsedSampleTime, ok := ProcessCPUSampleFromMetadata(metadata)
	require.True(t, ok)
	require.Equal(t, 12.5, cpuSeconds)
	require.Equal(t, sampleTime, parsedSampleTime)
}

func TestHeartbeatMetadataWithOptionsSkipsInvalidProcessCPUSample(t *testing.T) {
	metadata := HeartbeatMetadataWithOptions(nil, HeartbeatMetadataOptions{
		GoMaxProcs:        4,
		ProcessCPUSeconds: 12.5,
		HasProcessCPU:     true,
	})

	_, _, ok := ProcessCPUSampleFromMetadata(metadata)
	require.False(t, ok)
	require.NotContains(t, metadata, ProcessCPUSecondsMetadataKey)
	require.NotContains(t, metadata, SampleUnixNanosMetadataKey)
}

func TestProcessCPUSampleFromMetadataRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
	}{
		{
			name:     "missing cpu",
			metadata: map[string]string{SampleUnixNanosMetadataKey: "1"},
		},
		{
			name:     "missing sample time",
			metadata: map[string]string{ProcessCPUSecondsMetadataKey: "1"},
		},
		{
			name: "negative cpu",
			metadata: map[string]string{
				ProcessCPUSecondsMetadataKey: "-1",
				SampleUnixNanosMetadataKey:   "1",
			},
		},
		{
			name: "invalid cpu",
			metadata: map[string]string{
				ProcessCPUSecondsMetadataKey: "not-a-number",
				SampleUnixNanosMetadataKey:   "1",
			},
		},
		{
			name: "invalid sample time",
			metadata: map[string]string{
				ProcessCPUSecondsMetadataKey: "1",
				SampleUnixNanosMetadataKey:   "not-a-number",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, ok := ProcessCPUSampleFromMetadata(test.metadata)
			require.False(t, ok)
		})
	}
}
