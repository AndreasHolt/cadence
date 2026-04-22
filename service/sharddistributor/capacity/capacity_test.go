package capacity

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeartbeatMetadata(t *testing.T) {
	tests := []struct {
		name             string
		metadata         map[string]string
		goMaxProcs       int
		expectedMetadata map[string]string
	}{
		{
			name:       "adds gomaxprocs to empty metadata",
			metadata:   nil,
			goMaxProcs: 4,
			expectedMetadata: map[string]string{
				GoMaxProcsMetadataKey: "4",
			},
		},
		{
			name: "preserves existing metadata",
			metadata: map[string]string{
				"zone": "a",
			},
			goMaxProcs: 8,
			expectedMetadata: map[string]string{
				"zone":                "a",
				GoMaxProcsMetadataKey: "8",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			heartbeatMetadata := HeartbeatMetadata(tt.metadata, tt.goMaxProcs)

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
			name: "missing gomaxprocs key falls back to one",
			metadata: map[string]string{
				"zone": "a",
			},
			expectedWeight: 1,
		},
		{
			name: "invalid gomaxprocs falls back to one",
			metadata: map[string]string{
				GoMaxProcsMetadataKey: "invalid",
			},
			expectedWeight: 1,
		},
		{
			name: "non-positive gomaxprocs falls back to one",
			metadata: map[string]string{
				GoMaxProcsMetadataKey: "0",
			},
			expectedWeight: 1,
		},
		{
			name: "valid gomaxprocs becomes weight",
			metadata: map[string]string{
				GoMaxProcsMetadataKey: "8",
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

func TestLatencyEWmaMsFromMetadata(t *testing.T) {
	tests := []struct {
		name            string
		metadata        map[string]string
		expectedLatency float64
	}{
		{
			name:            "missing metadata falls back to zero",
			metadata:        nil,
			expectedLatency: 0,
		},
		{
			name: "missing latency key falls back to zero",
			metadata: map[string]string{
				"zone": "a",
			},
			expectedLatency: 0,
		},
		{
			name: "invalid latency falls back to zero",
			metadata: map[string]string{
				LatencyEWmaMsMetadataKey: "invalid",
			},
			expectedLatency: 0,
		},
		{
			name: "negative latency falls back to zero",
			metadata: map[string]string{
				LatencyEWmaMsMetadataKey: "-1",
			},
			expectedLatency: 0,
		},
		{
			name: "valid latency is parsed",
			metadata: map[string]string{
				LatencyEWmaMsMetadataKey: "12.5",
			},
			expectedLatency: 12.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expectedLatency, LatencyEWmaMsFromMetadata(tt.metadata))
		})
	}
}
