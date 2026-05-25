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

type executorCPUCostSmoothed struct {
	cost       float64
	busyCores  float64
	lastUpdate time.Time
}

type executorCPUDiagnostic struct {
	cost      float64
	busyCores float64
}

// CPUObservationState tracks previous executor CPU samples across rebalance cycles.
type CPUObservationState struct {
	samples       map[string]executorCPUSample
	smoothedCosts map[string]executorCPUCostSmoothed
	smoothingTau  time.Duration
}

// NewCPUObservationState creates state for CPU-seconds based greedy balancing.
func NewCPUObservationState() *CPUObservationState {
	return &CPUObservationState{
		samples:       make(map[string]executorCPUSample),
		smoothedCosts: make(map[string]executorCPUCostSmoothed),
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

func (s *CPUObservationState) updateExecutorCPUCostObservations(state *store.NamespaceState, loads map[string]float64) map[string]float64 {
	diagnostics := s.updateExecutorCPUDiagnostics(state, loads)
	costs := make(map[string]float64, len(diagnostics))
	for executorID, diagnostic := range diagnostics {
		costs[executorID] = diagnostic.cost
	}
	return costs
}

func (s *CPUObservationState) updateExecutorCPUDiagnostics(state *store.NamespaceState, loads map[string]float64) map[string]executorCPUDiagnostic {
	if s == nil {
		return nil
	}
	if s.samples == nil {
		s.samples = make(map[string]executorCPUSample)
	}

	diagnostics := make(map[string]executorCPUDiagnostic)
	seenExecutors := make(map[string]struct{}, len(state.Executors))
	for executorID, executor := range state.Executors {
		seenExecutors[executorID] = struct{}{}
		diagnostic, ok := s.updateExecutorCPUDiagnostic(executorID, executor.Metadata, loads[executorID])
		if ok {
			diagnostics[executorID] = diagnostic
		}
	}

	for executorID := range s.samples {
		if _, ok := seenExecutors[executorID]; !ok {
			delete(s.samples, executorID)
		}
	}
	for executorID := range s.smoothedCosts {
		if _, ok := seenExecutors[executorID]; !ok {
			delete(s.smoothedCosts, executorID)
		}
	}

	return diagnostics
}

func (s *CPUObservationState) updateExecutorCPUCostObservation(executorID string, metadata map[string]string, load float64) (float64, bool) {
	diagnostic, ok := s.updateExecutorCPUDiagnostic(executorID, metadata, load)
	if !ok {
		return 0, false
	}
	return diagnostic.cost, true
}

func (s *CPUObservationState) updateExecutorCPUDiagnostic(executorID string, metadata map[string]string, load float64) (executorCPUDiagnostic, bool) {
	if s.samples == nil {
		s.samples = make(map[string]executorCPUSample)
	}
	if s.smoothedCosts == nil {
		s.smoothedCosts = make(map[string]executorCPUCostSmoothed)
	}

	currentSample, ok := executorCPUSampleFromMetadata(metadata)
	if !ok {
		delete(s.samples, executorID)
		delete(s.smoothedCosts, executorID)
		return executorCPUDiagnostic{}, false
	}

	previousSample, hasPreviousSample := s.samples[executorID]
	s.samples[executorID] = currentSample
	if !hasPreviousSample {
		delete(s.smoothedCosts, executorID)
		return executorCPUDiagnostic{}, false
	}

	// Duplicate heartbeat sample (rebalance ran faster than heartbeat).
	// Preserve the smoothed CPU cost and return the last known value. Reusing
	// the cost avoids recomputing stale busy cores against a newer assignment load.
	if currentSample.sampleTime.Equal(previousSample.sampleTime) {
		prevSmoothed, hasPrevSmoothed := s.smoothedCosts[executorID]
		if hasPrevSmoothed {
			return executorCPUDiagnostic{cost: prevSmoothed.cost, busyCores: prevSmoothed.busyCores}, true
		}
		return executorCPUDiagnostic{}, false
	}

	busyCores, ok := computeExecutorCPUObservation(previousSample, currentSample)
	if !ok {
		delete(s.smoothedCosts, executorID)
		return executorCPUDiagnostic{}, false
	}
	if load <= 0 {
		delete(s.smoothedCosts, executorID)
		return executorCPUDiagnostic{}, false
	}
	cost := busyCores / load
	if math.IsNaN(cost) || math.IsInf(cost, 0) {
		delete(s.smoothedCosts, executorID)
		return executorCPUDiagnostic{}, false
	}

	if s.smoothingTau <= 0 {
		return executorCPUDiagnostic{cost: cost, busyCores: busyCores}, true
	}

	prevSmoothed, hasPrevSmoothed := s.smoothedCosts[executorID]
	if !hasPrevSmoothed {
		s.smoothedCosts[executorID] = executorCPUCostSmoothed{
			cost:       cost,
			busyCores:  busyCores,
			lastUpdate: currentSample.sampleTime,
		}
		return executorCPUDiagnostic{cost: cost, busyCores: busyCores}, true
	}

	newSmoothed, err := statistics.CalculateSmoothedLoadWithTau(prevSmoothed.cost, cost, prevSmoothed.lastUpdate, currentSample.sampleTime, s.smoothingTau)
	if err != nil {
		delete(s.smoothedCosts, executorID)
		return executorCPUDiagnostic{cost: cost, busyCores: busyCores}, true
	}

	s.smoothedCosts[executorID] = executorCPUCostSmoothed{
		cost:       newSmoothed,
		busyCores:  busyCores,
		lastUpdate: currentSample.sampleTime,
	}
	return executorCPUDiagnostic{cost: newSmoothed, busyCores: busyCores}, true
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
