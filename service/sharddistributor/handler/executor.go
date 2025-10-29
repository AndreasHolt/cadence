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

	// Unsure where to put this
	_ewmaAlpha = 0.1
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

	newHeartbeat := store.HeartbeatState{
		LastHeartbeat:  now.Unix(),
		Status:         request.Status,
		ReportedShards: request.ShardStatusReports,
	}

	err = h.storage.RecordHeartbeat(ctx, request.Namespace, request.ExecutorID, newHeartbeat)
	if err != nil {
		return nil, fmt.Errorf("record heartbeat: %w", err)
	}

	newStatistics, err := h.prepareNewStatistics(ctx, request, request.ShardStatusReports, assignedShards)
	if err != nil {
		return nil, fmt.Errorf("prepare new statistics: %w", err)
	}

	err = h.storage.UpdateShardStatistics(ctx, request.Namespace, request.ExecutorID, newStatistics)
	if err != nil {
		return nil, err
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

func (h *executor) prepareNewStatistics(ctx context.Context, request *types.ExecutorHeartbeatRequest, shardReports map[string]*types.ShardStatusReport, assignedState *store.AssignedState) (map[string]store.ShardStatistics, error) {
	state, err := h.storage.GetState(ctx, request.Namespace)
	if err != nil {
		return nil, err
	}

	newShardStatistics := make(map[string]store.ShardStatistics)
	for shardID, shardStat := range state.ShardStats {
		if _, ok := assignedState.AssignedShards[shardID]; !ok {
			// This shard is not assigned to the current executor, we can safely continue
			continue
		}
		newShardStat := shardStat

		report, ok := shardReports[shardID]
		if !ok {
			return nil, fmt.Errorf("Could not get report for assigned shard")
		}
		newShardStat.SmoothedLoad = _ewmaAlpha*report.ShardLoad + (1-_ewmaAlpha)*shardStat.SmoothedLoad
		newShardStat.LastUpdateTime = h.timeSource.Now().Unix()

		newShardStatistics[shardID] = newShardStat
	}

	return newShardStatistics, nil
}
