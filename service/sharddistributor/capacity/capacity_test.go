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
