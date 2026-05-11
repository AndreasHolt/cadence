package greedy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
