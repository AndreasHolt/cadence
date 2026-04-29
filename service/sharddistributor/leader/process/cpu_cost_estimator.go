package process

import (
	"math"

	"github.com/uber/cadence/service/sharddistributor/store"
)

const (
	executorCPUCostDecay           = 0.95
	minExecutorCPUCostSampleWeight = 10.0
	minExecutorCPUCostApplyWeight  = 60.0
	minExecutorCPUCostLoadVariance = 1e-9
	minExecutorCPUCostPerLoadUnit  = 1e-9
	minExecutorCPUCostCorrection   = 0.5
	maxExecutorCPUCostCorrection   = 2.0
)

type executorCPUCostState struct {
	sampleWeight     float64
	loadSum          float64
	busyCoresSum     float64
	loadBusyCoresSum float64
	loadSquaredSum   float64
}

type executorCPUCostEstimate struct {
	baselineBusyCores  float64
	cpuCostPerLoadUnit float64
	sampleWeight       float64
}

func (p *namespaceProcessor) updateExecutorCPUCostEstimates(
	loads map[string]float64,
	observations map[string]executorCPUObservation,
	namespaceState *store.NamespaceState,
) map[string]executorCPUCostEstimate {
	if p.executorCPUCostStates == nil {
		p.executorCPUCostStates = make(map[string]executorCPUCostState)
	}
	if p.executorCPUCostEstimates == nil {
		p.executorCPUCostEstimates = make(map[string]executorCPUCostEstimate)
	}

	for executorID, observation := range observations {
		load, ok := loads[executorID]
		if !ok {
			continue
		}
		estimate, ok := p.updateExecutorCPUCostEstimate(executorID, load, observation)
		if !ok {
			delete(p.executorCPUCostEstimates, executorID)
			continue
		}
		p.executorCPUCostEstimates[executorID] = estimate
	}

	for executorID := range p.executorCPUCostStates {
		executor, ok := namespaceState.Executors[executorID]
		if !ok {
			delete(p.executorCPUCostStates, executorID)
			delete(p.executorCPUCostEstimates, executorID)
			continue
		}
		if _, ok := executorCPUSampleFromMetadata(executor.Metadata); !ok {
			delete(p.executorCPUCostStates, executorID)
			delete(p.executorCPUCostEstimates, executorID)
		}
	}

	return p.executorCPUCostEstimates
}

func (p *namespaceProcessor) updateExecutorCPUCostEstimate(
	executorID string,
	load float64,
	observation executorCPUObservation,
) (executorCPUCostEstimate, bool) {
	if p.executorCPUCostStates == nil {
		p.executorCPUCostStates = make(map[string]executorCPUCostState)
	}
	if !validExecutorCPUCostInput(load, observation) {
		delete(p.executorCPUCostStates, executorID)
		return executorCPUCostEstimate{}, false
	}

	state := p.executorCPUCostStates[executorID]
	state = state.withObservation(load, observation)
	p.executorCPUCostStates[executorID] = state

	return state.estimate()
}

func validExecutorCPUCostInput(load float64, observation executorCPUObservation) bool {
	if load < 0 || math.IsNaN(load) || math.IsInf(load, 0) {
		return false
	}
	if observation.busyCores < 0 || math.IsNaN(observation.busyCores) || math.IsInf(observation.busyCores, 0) {
		return false
	}
	if observation.intervalSeconds <= 0 || math.IsNaN(observation.intervalSeconds) || math.IsInf(observation.intervalSeconds, 0) {
		return false
	}
	return true
}

func (s executorCPUCostState) withObservation(load float64, observation executorCPUObservation) executorCPUCostState {
	weight := observation.intervalSeconds

	s.sampleWeight *= executorCPUCostDecay
	s.loadSum *= executorCPUCostDecay
	s.busyCoresSum *= executorCPUCostDecay
	s.loadBusyCoresSum *= executorCPUCostDecay
	s.loadSquaredSum *= executorCPUCostDecay

	s.sampleWeight += weight
	s.loadSum += load * weight
	s.busyCoresSum += observation.busyCores * weight
	s.loadBusyCoresSum += load * observation.busyCores * weight
	s.loadSquaredSum += load * load * weight

	return s
}

func (s executorCPUCostState) estimate() (executorCPUCostEstimate, bool) {
	if s.sampleWeight < minExecutorCPUCostSampleWeight {
		return executorCPUCostEstimate{}, false
	}

	denominator := s.loadSquaredSum - (s.loadSum*s.loadSum)/s.sampleWeight
	if denominator <= minExecutorCPUCostLoadVariance || math.IsNaN(denominator) || math.IsInf(denominator, 0) {
		return executorCPUCostEstimate{}, false
	}

	numerator := s.loadBusyCoresSum - (s.loadSum*s.busyCoresSum)/s.sampleWeight
	cpuCostPerLoadUnit := numerator / denominator
	if cpuCostPerLoadUnit <= minExecutorCPUCostPerLoadUnit ||
		math.IsNaN(cpuCostPerLoadUnit) ||
		math.IsInf(cpuCostPerLoadUnit, 0) {
		return executorCPUCostEstimate{}, false
	}

	baselineBusyCores := (s.busyCoresSum - cpuCostPerLoadUnit*s.loadSum) / s.sampleWeight
	if math.IsNaN(baselineBusyCores) || math.IsInf(baselineBusyCores, 0) {
		return executorCPUCostEstimate{}, false
	}
	if baselineBusyCores < 0 {
		baselineBusyCores = 0
	}

	return executorCPUCostEstimate{
		baselineBusyCores:  baselineBusyCores,
		cpuCostPerLoadUnit: cpuCostPerLoadUnit,
		sampleWeight:       s.sampleWeight,
	}, true
}
