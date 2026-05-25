package greedy

import (
	"math"
	"slices"

	"github.com/uber/cadence/service/sharddistributor/capacity"
	"github.com/uber/cadence/service/sharddistributor/store"
)

// RuntimeState carries greedy balancer state across planning cycles.
type RuntimeState struct {
	CPUObservations *CPUObservationState
	LatencyCapacity *LatencyCapacityState
}

// NewRuntimeState creates runtime state for greedy planning.
func NewRuntimeState() *RuntimeState {
	return &RuntimeState{
		CPUObservations: NewCPUObservationState(),
		LatencyCapacity: NewLatencyCapacityState(),
	}
}

// LatencyCapacityState learns executor capacity weights from persistent latency differences.
type LatencyCapacityState struct {
	weights map[string]float64
}

// NewLatencyCapacityState creates adaptive latency-capacity state.
func NewLatencyCapacityState() *LatencyCapacityState {
	return &LatencyCapacityState{
		weights: make(map[string]float64),
	}
}

func (s *LatencyCapacityState) updateWeights(
	currentAssignments map[string][]string,
	state *store.NamespaceState,
) map[string]float64 {
	baseWeights, totalBase := baseCapacityWeights(currentAssignments, state)
	if totalBase <= 0 {
		return baseWeights
	}
	if s == nil {
		return computeLatencyAdjustedWeights(currentAssignments, state, baseWeights)
	}
	if s.weights == nil {
		s.weights = make(map[string]float64, len(currentAssignments))
	}

	for executorID := range s.weights {
		if _, ok := currentAssignments[executorID]; !ok {
			delete(s.weights, executorID)
		}
	}
	for executorID, baseWeight := range baseWeights {
		if existing := s.weights[executorID]; existing <= 0 || math.IsNaN(existing) || math.IsInf(existing, 0) {
			s.weights[executorID] = baseWeight
		}
	}

	latencies := executorLatencies(currentAssignments, state)
	if len(latencies) < 2 {
		return cloneFloatMap(s.weights)
	}

	medianLatency := medianPositiveValues(latencies)
	if medianLatency <= 0 {
		return cloneFloatMap(s.weights)
	}

	desired := make(map[string]float64, len(currentAssignments))
	for executorID, currentWeight := range s.weights {
		latencyMs, ok := latencies[executorID]
		if !ok || latencyMs <= 0 {
			desired[executorID] = currentWeight
			continue
		}
		relativeLatency := latencyMs / medianLatency
		if relativeLatency <= 0 || math.IsNaN(relativeLatency) || math.IsInf(relativeLatency, 0) {
			desired[executorID] = currentWeight
			continue
		}
		desired[executorID] = baseWeights[executorID] / relativeLatency
	}

	normalizeWeights(desired, totalBase, minLearnedWeight(currentAssignments, totalBase))

	maxStep := minLearnedWeight(currentAssignments, totalBase)
	next := make(map[string]float64, len(currentAssignments))
	for executorID, currentWeight := range s.weights {
		next[executorID] = moveToward(currentWeight, desired[executorID], maxStep)
	}
	normalizeWeights(next, totalBase, maxStep)
	s.weights = next
	return cloneFloatMap(s.weights)
}

func baseCapacityWeights(currentAssignments map[string][]string, state *store.NamespaceState) (map[string]float64, float64) {
	weights := make(map[string]float64, len(currentAssignments))
	total := 0.0
	for executorID := range currentAssignments {
		weight := capacity.WeightFromMetadata(state.Executors[executorID].Metadata)
		if weight <= 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
			weight = 1
		}
		weights[executorID] = weight
		total += weight
	}
	return weights, total
}

func executorLatencies(currentAssignments map[string][]string, state *store.NamespaceState) map[string]float64 {
	latencies := make(map[string]float64, len(currentAssignments))
	for executorID := range currentAssignments {
		latencyMs := capacity.LatencyEWmaMsFromMetadata(state.Executors[executorID].Metadata)
		if latencyMs > 0 && !math.IsNaN(latencyMs) && !math.IsInf(latencyMs, 0) {
			latencies[executorID] = latencyMs
		}
	}
	return latencies
}

func medianPositiveValues(valuesByKey map[string]float64) float64 {
	values := make([]float64, 0, len(valuesByKey))
	for _, value := range valuesByKey {
		if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
			values = append(values, value)
		}
	}
	if len(values) == 0 {
		return 0
	}
	slices.Sort(values)
	mid := len(values) / 2
	if len(values)%2 == 1 {
		return values[mid]
	}
	return (values[mid-1] + values[mid]) / 2
}

func minLearnedWeight(currentAssignments map[string][]string, totalBase float64) float64 {
	totalShards := 0
	for _, shards := range currentAssignments {
		totalShards += len(shards)
	}
	if totalShards <= 0 {
		totalShards = len(currentAssignments)
	}
	if totalShards <= 0 || totalBase <= 0 {
		return 1
	}
	return totalBase / float64(totalShards)
}

func moveToward(current, desired, maxStep float64) float64 {
	if desired > current+maxStep {
		return current + maxStep
	}
	if desired < current-maxStep {
		return current - maxStep
	}
	return desired
}

func normalizeWeights(weights map[string]float64, targetTotal, floor float64) {
	if len(weights) == 0 || targetTotal <= 0 {
		return
	}
	total := 0.0
	for key, value := range weights {
		if value < floor || math.IsNaN(value) || math.IsInf(value, 0) {
			value = floor
			weights[key] = value
		}
		total += value
	}
	if total <= 0 {
		equal := targetTotal / float64(len(weights))
		for key := range weights {
			weights[key] = equal
		}
		return
	}
	scale := targetTotal / total
	for key, value := range weights {
		weights[key] = value * scale
	}
}

func cloneFloatMap(values map[string]float64) map[string]float64 {
	cloned := make(map[string]float64, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
