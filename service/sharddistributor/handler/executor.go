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

	err = h.updateShardload(ctx, request, request.ShardStatusReports, assignedShards)
	if err != nil {
		return nil, fmt.Errorf("update shardload: %w", err)
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

func (h *executor) updateShardload(ctx context.Context, request *types.ExecutorHeartbeatRequest, shardReports map[string]*types.ShardStatusReport, assignedShards *store.AssignedState) error {
	state, err := h.storage.GetState(ctx, request.Namespace)
	if err != nil {
		return err
	}

	assignedShardsSet := make(map[string]struct{})
	if assignedShards != nil {
		for shardID := range assignedShards.AssignedShards {
			assignedShardsSet[shardID] = struct{}{}
		}
	}

	newShardMetrics := make(map[string]store.ShardMetrics)
	for shardID, shardMetric := range state.ShardMetrics {
		newShardMetric := shardMetric
		report, ok := shardReports[shardID]
		if !ok {
			continue
		}

		if _, ok := assignedShardsSet[shardID]; !ok {
			// This executor is reporting metrics for a shard that is not assigned to it.
			// This can happen due to race conditions where shard ownership changes.
			// We should not update metrics for such shards.
			continue
		}

		newShardMetric.SmoothedLoad = _ewmaAlpha*report.ShardLoad + (1-_ewmaAlpha)*shardMetric.SmoothedLoad
		newShardMetric.LastUpdateTime = h.timeSource.Now().Unix()

		newShardMetrics[shardID] = newShardMetric
	}

	if len(newShardMetrics) == 0 {
		return nil
	}

	err = h.storage.UpdateShardMetrics(ctx, request.Namespace, request.ExecutorID, newShardMetrics)
	if err != nil {
		return err
	}

	return nil
}
