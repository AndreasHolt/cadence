package capacity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHeartbeatMetadata(t *testing.T) {
	metadata := HeartbeatMetadata(map[string]string{"grpc_address": "127.0.0.1:7933"}, 4)

	assert.Equal(t, "127.0.0.1:7933", metadata["grpc_address"])
	assert.Equal(t, "4", metadata[GoMaxProcsMetadataKey])
}

func TestWeightFromMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		expected float64
	}{
		{
			name:     "missing metadata falls back to one",
			metadata: nil,
			expected: 1,
		},
		{
			name:     "missing gomaxprocs falls back to one",
			metadata: map[string]string{},
			expected: 1,
		},
		{
			name:     "invalid gomaxprocs falls back to one",
			metadata: map[string]string{GoMaxProcsMetadataKey: "not-a-number"},
			expected: 1,
		},
		{
			name:     "non-positive gomaxprocs falls back to one",
			metadata: map[string]string{GoMaxProcsMetadataKey: "0"},
			expected: 1,
		},
		{
			name:     "valid gomaxprocs is used as weight",
			metadata: map[string]string{GoMaxProcsMetadataKey: "8"},
			expected: 8,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, WeightFromMetadata(test.metadata))
		})
	}
}
