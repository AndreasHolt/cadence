package process

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/config"
	"github.com/uber/cadence/service/sharddistributor/store"
)

// TestGreedyLoadBalance_RebalanceCycleUsesSmoothedStats verifies GREEDY mode uses persisted shard statistics to plan moves.
func TestGreedyLoadBalance_RebalanceCycleUsesSmoothedStats(t *testing.T) {
	mocks := setupProcessorTest(t, config.NamespaceTypeEphemeral)
	defer mocks.ctrl.Finish()
	mocks.sdConfig.LoadBalancingMode = func(namespace string) string {
		return config.LoadBalancingModeGREEDY
	}
	processor := mocks.factory.CreateProcessor(mocks.cfg, mocks.store, mocks.election).(*namespaceProcessor)

	execA, execB := "exec-A", "exec-B"
	now := mocks.timeSource.Now()
	assignments := map[string]store.AssignedState{
		execA: {AssignedShards: make(map[string]*types.ShardAssignment)},
		execB: {AssignedShards: make(map[string]*types.ShardAssignment)},
	}
	shardStats := make(map[string]store.ShardStatistics)

	for i := range 50 {
		shardID := fmt.Sprintf("A-%d", i)
		assignments[execA].AssignedShards[shardID] = &types.ShardAssignment{Status: types.AssignmentStatusREADY}
		shardStats[shardID] = store.ShardStatistics{SmoothedLoad: 3.0, LastUpdateTime: now}
	}
	for i := range 50 {
		shardID := fmt.Sprintf("B-%d", i)
		assignments[execB].AssignedShards[shardID] = &types.ShardAssignment{Status: types.AssignmentStatusREADY}
		shardStats[shardID] = store.ShardStatistics{SmoothedLoad: 1.0, LastUpdateTime: now}
	}

	mocks.store.EXPECT().GetState(gomock.Any(), mocks.cfg.Name).Return(&store.NamespaceState{
		Executors: map[string]store.HeartbeatState{
			execA: {Status: types.ExecutorStatusACTIVE, LastHeartbeat: now},
			execB: {Status: types.ExecutorStatusACTIVE, LastHeartbeat: now},
		},
		ShardAssignments: assignments,
		ShardStats:       shardStats,
	}, nil)
	expectCurrentOwners(mocks.store, mocks.cfg.Name, assignments)
	mocks.election.EXPECT().Guard().Return(store.NopGuard())
	mocks.store.EXPECT().AssignShards(gomock.Any(), mocks.cfg.Name, gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, request store.AssignShardsRequest, _ store.GuardFunc) error {
			newAssignments := request.NewState.ShardAssignments
			assert.Less(t, len(newAssignments[execA].AssignedShards), 50)
			assert.Greater(t, len(newAssignments[execB].AssignedShards), 50)
			return nil
		},
	)

	require.NoError(t, processor.rebalanceShards(context.Background()))
}

// TestGreedyLoadBalance_SkipsNonBeneficialHotShard verifies the planner avoids moves that worsen load balance.
func TestGreedyLoadBalance_SkipsNonBeneficialHotShard(t *testing.T) {
	mocks := setupProcessorTest(t, config.NamespaceTypeEphemeral)
	defer mocks.ctrl.Finish()
	processor := mocks.factory.CreateProcessor(mocks.cfg, mocks.store, mocks.election).(*namespaceProcessor)
	now := mocks.timeSource.Now()

	currentAssignments := map[string][]string{
		"exec-A": {"hot", "warm"},
		"exec-B": {"b-1"},
	}
	namespaceState := &store.NamespaceState{
		Executors: map[string]store.HeartbeatState{
			"exec-A": {Status: types.ExecutorStatusACTIVE, LastHeartbeat: now},
			"exec-B": {Status: types.ExecutorStatusACTIVE, LastHeartbeat: now},
		},
		ShardAssignments: map[string]store.AssignedState{
			"exec-A": {AssignedShards: map[string]*types.ShardAssignment{"hot": {}, "warm": {}}},
			"exec-B": {AssignedShards: map[string]*types.ShardAssignment{"b-1": {}}},
		},
		ShardStats: map[string]store.ShardStatistics{
			"hot":  {SmoothedLoad: 10, LastUpdateTime: now},
			"warm": {SmoothedLoad: 2, LastUpdateTime: now},
			"b-1":  {SmoothedLoad: 3, LastUpdateTime: now},
		},
	}

	changed, err := processor.loadBalance(currentAssignments, namespaceState, nil, nil)
	require.NoError(t, err)
	require.True(t, changed)
	assert.True(t, slices.Contains(currentAssignments["exec-B"], "warm"))
	assert.False(t, slices.Contains(currentAssignments["exec-B"], "hot"))
}

// TestGreedyLoadBalance_RespectsMoveBudget verifies a cycle moves only the configured fraction of shards.
func TestGreedyLoadBalance_RespectsMoveBudget(t *testing.T) {
	mocks := setupProcessorTest(t, config.NamespaceTypeEphemeral)
	defer mocks.ctrl.Finish()
	processor := mocks.factory.CreateProcessor(mocks.cfg, mocks.store, mocks.election).(*namespaceProcessor)
	now := mocks.timeSource.Now()

	assignments := map[string]store.AssignedState{
		"exec-A": {AssignedShards: make(map[string]*types.ShardAssignment)},
		"exec-B": {AssignedShards: make(map[string]*types.ShardAssignment)},
	}
	currentAssignments := map[string][]string{
		"exec-A": {},
		"exec-B": {},
	}
	shardStats := make(map[string]store.ShardStatistics)
	for i := range 100 {
		shardID := fmt.Sprintf("A-%d", i)
		assignments["exec-A"].AssignedShards[shardID] = &types.ShardAssignment{Status: types.AssignmentStatusREADY}
		currentAssignments["exec-A"] = append(currentAssignments["exec-A"], shardID)
		shardStats[shardID] = store.ShardStatistics{SmoothedLoad: 2.0, LastUpdateTime: now}
	}
	for i := range 50 {
		shardID := fmt.Sprintf("B-%d", i)
		assignments["exec-B"].AssignedShards[shardID] = &types.ShardAssignment{Status: types.AssignmentStatusREADY}
		currentAssignments["exec-B"] = append(currentAssignments["exec-B"], shardID)
		shardStats[shardID] = store.ShardStatistics{SmoothedLoad: 0.1, LastUpdateTime: now}
	}
	namespaceState := &store.NamespaceState{
		Executors: map[string]store.HeartbeatState{
			"exec-A": {Status: types.ExecutorStatusACTIVE, LastHeartbeat: now},
			"exec-B": {Status: types.ExecutorStatusACTIVE, LastHeartbeat: now},
		},
		ShardAssignments: assignments,
		ShardStats:       shardStats,
	}

	expectedBudget := computeMoveBudget(len(shardStats), processor.cfg.LoadBalance.MoveBudgetProportion)
	changed, err := processor.loadBalance(currentAssignments, namespaceState, nil, nil)
	require.NoError(t, err)
	require.True(t, changed)
	assert.Len(t, currentAssignments["exec-A"], 100-expectedBudget)
	assert.Len(t, currentAssignments["exec-B"], 50+expectedBudget)
}

// TestGreedyLoadBalance_PerShardCooldownSkipsRecentMove verifies recently moved shards are not moved again.
func TestGreedyLoadBalance_PerShardCooldownSkipsRecentMove(t *testing.T) {
	mocks := setupProcessorTest(t, config.NamespaceTypeEphemeral)
	defer mocks.ctrl.Finish()
	processor := mocks.factory.CreateProcessor(mocks.cfg, mocks.store, mocks.election).(*namespaceProcessor)
	now := mocks.timeSource.Now()
	recentMove := now.Add(-processor.cfg.LoadBalance.PerShardCooldown / 2)

	currentAssignments := map[string][]string{
		"exec-A": {"hot-1", "hot-2", "a-1", "a-2", "a-3"},
		"exec-B": {"b-1", "b-2", "b-3", "b-4", "b-5"},
	}
	namespaceState := &store.NamespaceState{
		Executors: map[string]store.HeartbeatState{
			"exec-A": {Status: types.ExecutorStatusACTIVE, LastHeartbeat: now},
			"exec-B": {Status: types.ExecutorStatusACTIVE, LastHeartbeat: now},
		},
		ShardStats: map[string]store.ShardStatistics{
			"hot-1": {SmoothedLoad: 10.0, LastUpdateTime: now, LastMoveTime: recentMove},
			"hot-2": {SmoothedLoad: 9.0, LastUpdateTime: now},
			"a-1":   {SmoothedLoad: 1.0, LastUpdateTime: now},
			"a-2":   {SmoothedLoad: 1.0, LastUpdateTime: now},
			"a-3":   {SmoothedLoad: 1.0, LastUpdateTime: now},
			"b-1":   {SmoothedLoad: 0.1, LastUpdateTime: now},
			"b-2":   {SmoothedLoad: 0.1, LastUpdateTime: now},
			"b-3":   {SmoothedLoad: 0.1, LastUpdateTime: now},
			"b-4":   {SmoothedLoad: 0.1, LastUpdateTime: now},
			"b-5":   {SmoothedLoad: 0.1, LastUpdateTime: now},
		},
		ShardAssignments: map[string]store.AssignedState{
			"exec-A": {AssignedShards: map[string]*types.ShardAssignment{"hot-1": {}, "hot-2": {}, "a-1": {}, "a-2": {}, "a-3": {}}},
			"exec-B": {AssignedShards: map[string]*types.ShardAssignment{"b-1": {}, "b-2": {}, "b-3": {}, "b-4": {}, "b-5": {}}},
		},
	}

	changed, err := processor.loadBalance(currentAssignments, namespaceState, nil, nil)
	require.NoError(t, err)
	require.True(t, changed)
	assert.True(t, slices.Contains(currentAssignments["exec-B"], "hot-2"))
	assert.False(t, slices.Contains(currentAssignments["exec-B"], "hot-1"))
}

// TestGreedyLoadBalance_NoDestinationsNotSevere verifies the destination fallback requires severe imbalance.
func TestGreedyLoadBalance_NoDestinationsNotSevere(t *testing.T) {
	mocks := setupProcessorTest(t, config.NamespaceTypeEphemeral)
	defer mocks.ctrl.Finish()
	processor := mocks.factory.CreateProcessor(mocks.cfg, mocks.store, mocks.election).(*namespaceProcessor)
	processor.cfg.LoadBalance.HysteresisLowerBand = 0.1
	processor.cfg.LoadBalance.SevereImbalanceRatio = 10.0
	now := mocks.timeSource.Now()

	currentAssignments := map[string][]string{
		"exec-A": {"a-1", "a-2", "a-3", "a-4", "a-5"},
		"exec-B": {"b-1"},
	}
	namespaceState := &store.NamespaceState{
		Executors: map[string]store.HeartbeatState{
			"exec-A": {Status: types.ExecutorStatusACTIVE, LastHeartbeat: now},
			"exec-B": {Status: types.ExecutorStatusACTIVE, LastHeartbeat: now},
		},
		ShardAssignments: map[string]store.AssignedState{
			"exec-A": {AssignedShards: map[string]*types.ShardAssignment{"a-1": {}, "a-2": {}, "a-3": {}, "a-4": {}, "a-5": {}}},
			"exec-B": {AssignedShards: map[string]*types.ShardAssignment{"b-1": {}}},
		},
		ShardStats: map[string]store.ShardStatistics{
			"a-1": {SmoothedLoad: 10, LastUpdateTime: now},
			"a-2": {SmoothedLoad: 10, LastUpdateTime: now},
			"a-3": {SmoothedLoad: 10, LastUpdateTime: now},
			"a-4": {SmoothedLoad: 10, LastUpdateTime: now},
			"a-5": {SmoothedLoad: 10, LastUpdateTime: now},
			"b-1": {SmoothedLoad: 25, LastUpdateTime: now},
		},
	}

	changed, err := processor.loadBalance(currentAssignments, namespaceState, nil, nil)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Len(t, currentAssignments["exec-A"], 5)
	assert.Len(t, currentAssignments["exec-B"], 1)
}

func expectCurrentOwners(mockStore *store.MockStore, namespace string, assignments map[string]store.AssignedState) {
	for executorID, assignment := range assignments {
		for shardID := range assignment.AssignedShards {
			mockStore.EXPECT().
				GetShardOwner(gomock.Any(), namespace, shardID).
				Return(&store.ShardOwner{ExecutorID: executorID}, nil).
				AnyTimes()
		}
	}
}
