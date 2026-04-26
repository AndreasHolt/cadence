package process

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

type executorCPUObservation struct {
	busyCores float64
}

func (p *namespaceProcessor) updateExecutorCPUObservations(namespaceState *store.NamespaceState) map[string]executorCPUObservation {
	if p.executorCPUSamples == nil {
		p.executorCPUSamples = make(map[string]executorCPUSample)
	}
	if p.executorCPUObservations == nil {
		p.executorCPUObservations = make(map[string]executorCPUObservation)
	}

	seenExecutors := make(map[string]struct{}, len(namespaceState.Executors))
	for executorID, executor := range namespaceState.Executors {
		seenExecutors[executorID] = struct{}{}
		observation, ok := p.updateExecutorCPUObservation(executorID, executor.Metadata)
		if !ok {
			delete(p.executorCPUObservations, executorID)
			continue
		}
		p.executorCPUObservations[executorID] = observation
	}

	for executorID := range p.executorCPUSamples {
		if _, ok := seenExecutors[executorID]; !ok {
			delete(p.executorCPUSamples, executorID)
			delete(p.executorCPUObservations, executorID)
		}
	}

	return p.executorCPUObservations
}

func (p *namespaceProcessor) updateExecutorCPUObservation(executorID string, metadata map[string]string) (executorCPUObservation, bool) {
	if p.executorCPUSamples == nil {
		p.executorCPUSamples = make(map[string]executorCPUSample)
	}

	currentSample, ok := executorCPUSampleFromMetadata(metadata)
	if !ok {
		delete(p.executorCPUSamples, executorID)
		return executorCPUObservation{}, false
	}

	previousSample, hasPreviousSample := p.executorCPUSamples[executorID]
	p.executorCPUSamples[executorID] = currentSample
	if !hasPreviousSample {
		return executorCPUObservation{}, false
	}

	return computeExecutorCPUObservation(previousSample, currentSample)
}

func executorCPUSampleFromMetadata(metadata map[string]string) (executorCPUSample, bool) {
	runtimeMetadata, ok := capacity.RuntimeMetadataFromMetadata(metadata)
	if !ok || runtimeMetadata.ProcessCPUSeconds == nil || runtimeMetadata.SampleUnixNanos == nil {
		return executorCPUSample{}, false
	}
	if *runtimeMetadata.ProcessCPUSeconds < 0 || math.IsNaN(*runtimeMetadata.ProcessCPUSeconds) || math.IsInf(*runtimeMetadata.ProcessCPUSeconds, 0) {
		return executorCPUSample{}, false
	}

	return executorCPUSample{
		processCPUSeconds: *runtimeMetadata.ProcessCPUSeconds,
		sampleTime:        time.Unix(0, *runtimeMetadata.SampleUnixNanos).UTC(),
	}, true
}

func computeExecutorCPUObservation(previousSample, currentSample executorCPUSample) (executorCPUObservation, bool) {
	deltaCPUSeconds := currentSample.processCPUSeconds - previousSample.processCPUSeconds
	if deltaCPUSeconds < 0 || math.IsNaN(deltaCPUSeconds) || math.IsInf(deltaCPUSeconds, 0) {
		return executorCPUObservation{}, false
	}

	deltaTimeSeconds := currentSample.sampleTime.Sub(previousSample.sampleTime).Seconds()
	if deltaTimeSeconds <= 0 || math.IsNaN(deltaTimeSeconds) || math.IsInf(deltaTimeSeconds, 0) {
		return executorCPUObservation{}, false
	}

	busyCores := deltaCPUSeconds / deltaTimeSeconds
	if math.IsNaN(busyCores) || math.IsInf(busyCores, 0) {
		return executorCPUObservation{}, false
	}

	return executorCPUObservation{busyCores: busyCores}, true
}
