package matching

import (
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service"
)

const (
	routingPathHashRing         = "hash_ring"
	routingPathShardDistributor = "shard_distributor"
)

// TaskListRouter finds the matching host that owns a task list.
type TaskListRouter struct {
	resolver               membership.Resolver
	taskListDifferentiator *TaskListDifferentiator
	metricsScope           metrics.Scope
}

func NewTaskListRouter(
	resolver membership.Resolver,
	taskListDifferentiator *TaskListDifferentiator,
	metricsClient metrics.Client,
) *TaskListRouter {
	return &TaskListRouter{
		resolver:               resolver,
		taskListDifferentiator: taskListDifferentiator,
		metricsScope:           metricsClient.Scope(metrics.ShardDistributorResolverScope),
	}
}

func (r *TaskListRouter) FromTaskList(taskListName string) (membership.HostInfo, error) {
	if r.taskListDifferentiator.TaskListExcludedFromShardDistributor(taskListName) {
		r.emitRoutingPathMetric(routingPathHashRing)
		return r.resolver.Lookup(service.Matching, taskListName)
	}

	r.emitRoutingPathMetric(routingPathShardDistributor)
	return r.resolver.Lookup(membership.MatchingShards, r.taskListDifferentiator.ShardID(taskListName))
}

func (r *TaskListRouter) emitRoutingPathMetric(path string) {
	r.metricsScope.Tagged(metrics.RoutingPathTag(path)).IncCounter(metrics.ShardDistributorResolverLookups)
}
