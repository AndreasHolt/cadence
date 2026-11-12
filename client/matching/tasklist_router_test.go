package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service"
)

func TestTaskListRouter_FromTaskList(t *testing.T) {
	tests := []struct {
		name                       string
		taskListName               string
		excludeShortLivedTaskLists bool
		shardTaskListsByHash       bool
		percentageOnboarded        int
		wantLookupService          string
		wantLookupKey              string
		wantRoutingPath            string
	}{
		{
			name:                "task list that is not onboarded is looked up on the hash ring by name",
			taskListName:        "test-key",
			percentageOnboarded: 0,
			wantLookupService:   service.Matching,
			wantLookupKey:       "test-key",
			wantRoutingPath:     routingPathHashRing,
		},
		{
			name:                "onboarded task list is looked up by its own shard",
			taskListName:        "test-key",
			percentageOnboarded: 100,
			wantLookupService:   membership.MatchingShards,
			wantLookupKey:       "test-key",
			wantRoutingPath:     routingPathShardDistributor,
		},
		{
			name:                 "onboarded task list with fixed shards is looked up by its hashed shard",
			taskListName:         "test-key",
			shardTaskListsByHash: true,
			percentageOnboarded:  100,
			wantLookupService:    membership.MatchingShards,
			wantLookupKey:        "7126", // farm.Fingerprint64([]byte("test-key"))%8192
			wantRoutingPath:      routingPathShardDistributor,
		},
		{
			name:                       "short-lived task list is looked up on the hash ring by name",
			taskListName:               "tasklist-550e8400-e29b-41d4-a716-446655440000",
			excludeShortLivedTaskLists: true,
			percentageOnboarded:        100,
			wantLookupService:          service.Matching,
			wantLookupKey:              "tasklist-550e8400-e29b-41d4-a716-446655440000",
			wantRoutingPath:            routingPathHashRing,
		},
		{
			name:                       "task list with a UUID is onboarded when short-lived task lists are not excluded",
			taskListName:               "tasklist-550e8400-e29b-41d4-a716-446655440000",
			excludeShortLivedTaskLists: false,
			percentageOnboarded:        100,
			wantLookupService:          membership.MatchingShards,
			wantLookupKey:              "tasklist-550e8400-e29b-41d4-a716-446655440000",
			wantRoutingPath:            routingPathShardDistributor,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := membership.NewMockResolver(gomock.NewController(t))
			resolver.EXPECT().Lookup(tt.wantLookupService, tt.wantLookupKey).Return(membership.NewHostInfo("owner-addr"), nil)

			testScope := tally.NewTestScope("", nil)
			router := NewTaskListRouter(
				resolver,
				NewTaskListDifferentiator(
					dynamicproperties.GetBoolPropertyFn(tt.excludeShortLivedTaskLists),
					dynamicproperties.GetBoolPropertyFn(tt.shardTaskListsByHash),
					membership.StaticPercentageOnboarded(tt.percentageOnboarded),
					config.DefaultNumMatchingShards,
				),
				metrics.NewClient(testScope, metrics.Matching, metrics.MigrationConfig{}),
			)

			host, err := router.FromTaskList(tt.taskListName)
			require.NoError(t, err)
			assert.Equal(t, "owner-addr", host.GetAddress())

			counters := testScope.Snapshot().Counters()
			require.Len(t, counters, 1)
			for _, counter := range counters {
				assert.Equal(t, "shard_distributor_resolver_lookups", counter.Name())
				assert.Equal(t, tt.wantRoutingPath, counter.Tags()["routing_path"])
				assert.Equal(t, int64(1), counter.Value())
			}
		})
	}
}

func TestTaskListRouter_FromTaskList_LookupFails(t *testing.T) {
	resolver := membership.NewMockResolver(gomock.NewController(t))
	resolver.EXPECT().Lookup(service.Matching, "test-key").Return(membership.HostInfo{}, assert.AnError)

	_, err := newRingOnlyTaskListRouter(resolver).FromTaskList("test-key")
	assert.ErrorIs(t, err, assert.AnError)
}

// newRingOnlyTaskListRouter returns a router that looks up every task list on the resolver's hash ring.
func newRingOnlyTaskListRouter(resolver membership.Resolver) *TaskListRouter {
	taskListDifferentiator := NewTaskListDifferentiator(
		dynamicproperties.GetBoolPropertyFn(false),
		dynamicproperties.GetBoolPropertyFn(false),
		membership.StaticPercentageOnboarded(0),
		config.DefaultNumMatchingShards,
	)
	return NewTaskListRouter(resolver, taskListDifferentiator, metrics.NewNoopMetricsClient())
}
