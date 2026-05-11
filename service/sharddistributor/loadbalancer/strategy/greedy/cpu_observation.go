package greedy

import (
	"math"
	"time"

	"github.com/uber/cadence/service/sharddistributor/capacity"
	"github.com/uber/cadence/service/sharddistributor/store"
)

type executorCPUSample struct {
	processCPUSeconds float64
	sampleTime        time.Time
}

// CPUObservationState tracks previous executor CPU samples across rebalance cycles.
type CPUObservationState struct {
	samples map[string]executorCPUSample
}

// NewCPUObservationState creates state for CPU-seconds based greedy balancing.
func NewCPUObservationState() *CPUObservationState {
	return &CPUObservationState{
		samples: make(map[string]executorCPUSample),
	}
}

func (s *CPUObservationState) updateExecutorCPUObservations(state *store.NamespaceState) map[string]float64 {
	if s == nil {
		return nil
	}
	if s.samples == nil {
		s.samples = make(map[string]executorCPUSample)
	}

	busyCoresMap := make(map[string]float64)
	seenExecutors := make(map[string]struct{}, len(state.Executors))
	for executorID, executor := range state.Executors {
		seenExecutors[executorID] = struct{}{}
		busyCores, ok := s.updateExecutorCPUObservation(executorID, executor.Metadata)
		if ok {
			busyCoresMap[executorID] = busyCores
		}
	}

	for executorID := range s.samples {
		if _, ok := seenExecutors[executorID]; !ok {
			delete(s.samples, executorID)
		}
	}

	return busyCoresMap
}

func (s *CPUObservationState) updateExecutorCPUObservation(executorID string, metadata map[string]string) (float64, bool) {
	if s.samples == nil {
		s.samples = make(map[string]executorCPUSample)
	}

	currentSample, ok := executorCPUSampleFromMetadata(metadata)
	if !ok {
		delete(s.samples, executorID)
		return 0, false
	}

	previousSample, hasPreviousSample := s.samples[executorID]
	s.samples[executorID] = currentSample
	if !hasPreviousSample {
		return 0, false
	}

	return computeExecutorCPUObservation(previousSample, currentSample)
}

func executorCPUSampleFromMetadata(metadata map[string]string) (executorCPUSample, bool) {
	processCPUSeconds, sampleTime, ok := capacity.ProcessCPUSampleFromMetadata(metadata)
	if !ok {
		return executorCPUSample{}, false
	}

	return executorCPUSample{
		processCPUSeconds: processCPUSeconds,
		sampleTime:        sampleTime,
	}, true
}

func computeExecutorCPUObservation(previousSample, currentSample executorCPUSample) (float64, bool) {
	deltaCPUSeconds := currentSample.processCPUSeconds - previousSample.processCPUSeconds
	if deltaCPUSeconds < 0 || math.IsNaN(deltaCPUSeconds) || math.IsInf(deltaCPUSeconds, 0) {
		return 0, false
	}

	deltaTimeSeconds := currentSample.sampleTime.Sub(previousSample.sampleTime).Seconds()
	if deltaTimeSeconds <= 0 || math.IsNaN(deltaTimeSeconds) || math.IsInf(deltaTimeSeconds, 0) {
		return 0, false
	}

	busyCores := deltaCPUSeconds / deltaTimeSeconds
	if math.IsNaN(busyCores) || math.IsInf(busyCores, 0) {
		return 0, false
	}

	return busyCores, true
}
