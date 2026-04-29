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

func (p *namespaceProcessor) updateExecutorCPUObservations(namespaceState *store.NamespaceState) map[string]float64 {
	if p.executorCPUSamples == nil {
		p.executorCPUSamples = make(map[string]executorCPUSample)
	}

	busyCoresMap := make(map[string]float64)
	seenExecutors := make(map[string]struct{}, len(namespaceState.Executors))
	
	for executorID, executor := range namespaceState.Executors {
		seenExecutors[executorID] = struct{}{}
		busyCores, ok := p.updateExecutorCPUObservation(executorID, executor.Metadata)
		if ok {
			busyCoresMap[executorID] = busyCores
		}
	}

	for executorID := range p.executorCPUSamples {
		if _, ok := seenExecutors[executorID]; !ok {
			delete(p.executorCPUSamples, executorID)
		}
	}

	return busyCoresMap
}

func (p *namespaceProcessor) updateExecutorCPUObservation(executorID string, metadata map[string]string) (float64, bool) {
	if p.executorCPUSamples == nil {
		p.executorCPUSamples = make(map[string]executorCPUSample)
	}

	currentSample, ok := executorCPUSampleFromMetadata(metadata)
	if !ok {
		delete(p.executorCPUSamples, executorID)
		return 0, false
	}

	previousSample, hasPreviousSample := p.executorCPUSamples[executorID]
	p.executorCPUSamples[executorID] = currentSample
	if !hasPreviousSample {
		return 0, false
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
