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
	_ewmaAlpha            = 0.1
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
	// If it's nil (e.g., on first heartbeat), it might have no assigned shards yet.
	if assignedShards == nil {
		// Initialize it to an empty struct to prevent a panic.
		// This ensures the loop below doesn't run, and shardList remains empty, which is correct.
		assignedShards = &store.AssignedState{}
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

	var shardList []string
	for id, _ := range assignedShards.AssignedShards {
		shardList = append(shardList, id)
	}

	prevSmoothLoadMap, err := h.storage.GetShardLoadMap(ctx, request.Namespace, shardList)
	if err != nil {

		return nil, fmt.Errorf("record heartbeat: %w", err)
	}
	smoothLoadShardMap := make(map[string]float64)
	// Different action depending on if it is the first heartbeat
	if previousHeartbeat == nil {
		for shardId, statusReport := range request.ShardStatusReports {
			smoothLoadShardMap[shardId] = statusReport.ShardLoad
		}
	} else {
		for shardId, statusReport := range request.ShardStatusReports {
			newVal := _ewmaAlpha*statusReport.ShardLoad + (1-_ewmaAlpha)*prevSmoothLoadMap[shardId]
			smoothLoadShardMap[shardId] = newVal
		}
	}

	newHeartbeat := store.HeartbeatState{
		LastHeartbeat:  now.Unix(),
		Status:         request.Status,
		ReportedShards: request.ShardStatusReports,
	}

	err = h.storage.RecordHeartbeat(ctx, request.Namespace, request.ExecutorID, newHeartbeat)
	if err != nil {
		return nil, fmt.Errorf("record heartbeat: %w", err)
	}

	err = h.storage.RecordShardLoadMap(ctx, request.Namespace, smoothLoadShardMap)
	if err != nil {
		return nil, fmt.Errorf("record smooth load: %w", err)
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
