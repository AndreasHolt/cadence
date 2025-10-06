package handler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/store"
)

const (
	_heartbeatRefreshRate = 2 * time.Second
	_smoothLoadAlpha      = 0.1
)

type executor struct {
	timeSource clock.TimeSource
	storage    store.Store
}

func NewExecutorHandler(storage store.Store,
	timeSource clock.TimeSource,
) Executor {
	return &executor{
		timeSource: timeSource,
		storage:    storage,
	}
}

func (h *executor) Heartbeat(ctx context.Context, request *types.ExecutorHeartbeatRequest) (*types.ExecutorHeartbeatResponse, error) {
	previousHeartbeat, assignedShards, err := h.storage.GetHeartbeat(ctx, request.Namespace, request.ExecutorID)
	// We ignore Executor not found errors, since it just means that this executor heartbeat the first time.
	if err != nil && !errors.Is(err, store.ErrExecutorNotFound) {
		return nil, fmt.Errorf("get heartbeat: %w", err)
	}

	now := h.timeSource.Now().UTC()

	// If the state has changed we need to update heartbeat data.
	// Otherwise, we want to do it with controlled frequency - at most every _heartbeatRefreshRate.
	if previousHeartbeat != nil && request.Status == previousHeartbeat.Status {
		lastHeartbeatTime := time.Unix(previousHeartbeat.LastHeartbeat, 0)
		if now.Sub(lastHeartbeatTime) < _heartbeatRefreshRate {
			return _convertResponse(assignedShards), nil
		}
	}

	// get the previous reports so we can update the smoothed load
	var previousReports map[string]*types.ShardStatusReport
	if previousHeartbeat != nil {
		previousReports = previousHeartbeat.ReportedShards
	}

	// update the smoothed load (weighted average) now that we have the latest reports
	smoothedReports, aggregatedLoad := smoothShardLoads(previousReports, request.ShardStatusReports)

	newHeartbeat := store.HeartbeatState{
		LastHeartbeat:  now.Unix(),
		Status:         request.Status,
		ReportedShards: smoothedReports,
		AggregatedLoad: aggregatedLoad,
	}

	err = h.storage.RecordHeartbeat(ctx, request.Namespace, request.ExecutorID, newHeartbeat)
	if err != nil {
		return nil, fmt.Errorf("record heartbeat: %w", err)
	}

	return _convertResponse(assignedShards), nil
}

func _convertResponse(shards *store.AssignedState) *types.ExecutorHeartbeatResponse {
	res := &types.ExecutorHeartbeatResponse{}
	if shards == nil {
		return res
	}
	res.ShardAssignments = shards.AssignedShards
	return res
}

// smoothShardLoads applies EWMA smoothing and persists the output so the next call uses the previous smoothed value.
func smoothShardLoads(
	previous map[string]*types.ShardStatusReport,
	current map[string]*types.ShardStatusReport,
) (map[string]*types.ShardStatusReport, float64) {
	if len(current) == 0 {
		return nil, 0
	}

	result := make(map[string]*types.ShardStatusReport, len(current))
	var aggregated float64

	for shardID, report := range current {
		if report == nil {
			continue
		}

		smoothedLoad := report.ShardLoad
		if prevReport, ok := previous[shardID]; ok && prevReport != nil {
			smoothedLoad = _smoothLoadAlpha*report.ShardLoad + (1-_smoothLoadAlpha)*prevReport.ShardLoad
		}

		result[shardID] = &types.ShardStatusReport{
			Status:    report.Status,
			ShardLoad: smoothedLoad,
		}
		aggregated += smoothedLoad
	}

	if len(result) == 0 {
		return nil, 0
	}

	return result, aggregated
}
