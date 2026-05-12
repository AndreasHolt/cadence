package greedy

import (
	"math"
	"time"

	"github.com/uber/cadence/service/sharddistributor/capacity"
	"github.com/uber/cadence/service/sharddistributor/statistics"
	"github.com/uber/cadence/service/sharddistributor/store"
)

type executorCPUSample struct {
	processCPUSeconds float64
	sampleTime        time.Time
}

type executorCPUSmoothed struct {
	busyCores  float64
	lastUpdate time.Time
}

// CPUObservationState tracks previous executor CPU samples across rebalance cycles.
type CPUObservationState struct {
	samples       map[string]executorCPUSample
	smoothed      map[string]executorCPUSmoothed
	smoothingTau  time.Duration
}

// NewCPUObservationState creates state for CPU-seconds based greedy balancing.
func NewCPUObservationState() *CPUObservationState {
	return &CPUObservationState{
		samples:  make(map[string]executorCPUSample),
		smoothed: make(map[string]executorCPUSmoothed),
	}
}

// SetSmoothingTau configures the EWMA time constant for CPU capacity smoothing.
// A value <= 0 disables smoothing and returns raw per-interval busy-cores rates.
func (s *CPUObservationState) SetSmoothingTau(tau time.Duration) {
	if s == nil {
		return
	}
	s.smoothingTau = tau
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
	for executorID := range s.smoothed {
		if _, ok := seenExecutors[executorID]; !ok {
			delete(s.smoothed, executorID)
		}
	}

	return busyCoresMap
}

func (s *CPUObservationState) updateExecutorCPUObservation(executorID string, metadata map[string]string) (float64, bool) {
	if s.samples == nil {
		s.samples = make(map[string]executorCPUSample)
	}
	if s.smoothed == nil {
		s.smoothed = make(map[string]executorCPUSmoothed)
	}

	currentSample, ok := executorCPUSampleFromMetadata(metadata)
	if !ok {
		delete(s.samples, executorID)
		delete(s.smoothed, executorID)
		return 0, false
	}

	previousSample, hasPreviousSample := s.samples[executorID]
	s.samples[executorID] = currentSample
	if !hasPreviousSample {
		delete(s.smoothed, executorID)
		return 0, false
	}

	busyCores, ok := computeExecutorCPUObservation(previousSample, currentSample)
	if !ok {
		delete(s.smoothed, executorID)
		return 0, false
	}

	if s.smoothingTau <= 0 {
		return busyCores, true
	}

	prevSmoothed, hasPrevSmoothed := s.smoothed[executorID]
	if !hasPrevSmoothed {
		s.smoothed[executorID] = executorCPUSmoothed{
			busyCores:  busyCores,
			lastUpdate: currentSample.sampleTime,
		}
		return busyCores, true
	}

	newSmoothed, err := statistics.CalculateSmoothedLoadWithTau(prevSmoothed.busyCores, busyCores, prevSmoothed.lastUpdate, currentSample.sampleTime, s.smoothingTau)
	if err != nil {
		delete(s.smoothed, executorID)
		return busyCores, true
	}

	s.smoothed[executorID] = executorCPUSmoothed{
		busyCores:  newSmoothed,
		lastUpdate: currentSample.sampleTime,
	}
	return newSmoothed, true
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
