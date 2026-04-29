package capacity

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common"
)

func TestHeartbeatMetadata(t *testing.T) {
	tests := []struct {
		name             string
		metadata         map[string]string
		options          HeartbeatMetadataOptions
		expectedMetadata map[string]string
	}{
		{
			name:     "adds runtime metadata to empty metadata",
			metadata: nil,
			options: HeartbeatMetadataOptions{
				GoMaxProcs: 4,
			},
			expectedMetadata: map[string]string{
				RuntimeMetadataKey: `{"gomaxprocs":4}`,
			},
		},
		{
			name: "preserves existing metadata",
			metadata: map[string]string{
				"zone": "a",
			},
			options: HeartbeatMetadataOptions{
				GoMaxProcs:        8,
				ProcessCPUSeconds: 12.5,
				HasProcessCPU:     true,
				SampleUnixNanos:   123,
			},
			expectedMetadata: map[string]string{
				"zone":             "a",
				RuntimeMetadataKey: `{"gomaxprocs":8,"process_cpu_seconds":12.5,"sample_unix_nanos":123}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			heartbeatMetadata := HeartbeatMetadata(tt.metadata, tt.options)

			require.Equal(t, tt.expectedMetadata, heartbeatMetadata)
			heartbeatMetadata["new-key"] = "new-value"
			require.NotContains(t, tt.metadata, "new-key")
		})
	}
}

func TestWeightFromMetadata(t *testing.T) {
	tests := []struct {
		name           string
		metadata       map[string]string
		expectedWeight float64
	}{
		{
			name:           "missing metadata falls back to one",
			metadata:       nil,
			expectedWeight: 1,
		},
		{
			name: "missing runtime metadata key falls back to one",
			metadata: map[string]string{
				"zone": "a",
			},
			expectedWeight: 1,
		},
		{
			name: "invalid runtime metadata falls back to one",
			metadata: map[string]string{
				RuntimeMetadataKey: "invalid",
			},
			expectedWeight: 1,
		},
		{
			name: "non-positive gomaxprocs falls back to one",
			metadata: map[string]string{
				RuntimeMetadataKey: `{"gomaxprocs":0}`,
			},
			expectedWeight: 1,
		},
		{
			name: "valid gomaxprocs becomes weight",
			metadata: map[string]string{
				RuntimeMetadataKey: `{"gomaxprocs":8}`,
			},
			expectedWeight: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expectedWeight, WeightFromMetadata(tt.metadata))
		})
	}
}

func TestRuntimeMetadataFromMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		want     RuntimeMetadata
		wantOK   bool
	}{
		{
			name: "missing metadata",
		},
		{
			name: "invalid json",
			metadata: map[string]string{
				RuntimeMetadataKey: "invalid",
			},
		},
		{
			name: "valid runtime metadata",
			metadata: map[string]string{
				RuntimeMetadataKey: `{"gomaxprocs":4,"process_cpu_seconds":10.25,"sample_unix_nanos":123}`,
			},
			want: RuntimeMetadata{
				GoMaxProcs:        4,
				ProcessCPUSeconds: common.Float64Ptr(10.25),
				SampleUnixNanos:   common.Int64Ptr(123),
			},
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := RuntimeMetadataFromMetadata(tt.metadata)

			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.want, got)
		})
	}
}
