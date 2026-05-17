package loadbalancer

import (
	"fmt"
	"time"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/config"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/plan"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/strategy/greedy"
	"github.com/uber/cadence/service/sharddistributor/loadbalancer/strategy/naive"
	"github.com/uber/cadence/service/sharddistributor/store"
)

// RuntimeState carries load-balancer state that must survive across planning cycles.
type RuntimeState struct {
	GreedyCPUObservations *greedy.CPUObservationState
}

// NewRuntimeState creates runtime state for load-balancer planning.
func NewRuntimeState() RuntimeState {
	return RuntimeState{
		GreedyCPUObservations: greedy.NewCPUObservationState(),
	}
}

// PlanInitialPlacement returns planned placements for a batch of unassigned shards.
func PlanInitialPlacement(
	cfg *config.Config,
	namespace string,
	state *store.NamespaceState,
	shardIDs []string,
) ([]plan.Placement, error) {
	mode := cfg.GetLoadBalancingMode(namespace)
	switch mode {
	case types.LoadBalancingModeNAIVE:
		return naive.PlanInitialPlacement(state, shardIDs)
	case types.LoadBalancingModeGREEDY:
		return greedy.PlanInitialPlacement(state, shardIDs)
	default:
		return nil, fmt.Errorf("unsupported load balancing mode: %s", mode)
	}
}

// PlanRebalance returns planned shard moves for the current assignment state.
func PlanRebalance(
	cfg *config.Config,
	namespace string,
	state *store.NamespaceState,
	currentAssignments map[string][]string,
	now time.Time,
	shardStatsStaleAfter time.Duration,
	logger log.Logger,
	metricsScope metrics.Scope,
	runtimeState ...*RuntimeState,
) ([]plan.Move, error) {
	mode := cfg.GetLoadBalancingMode(namespace)
	switch mode {
	case types.LoadBalancingModeNAIVE:
		return naive.PlanRebalance(cfg.LoadBalancingNaive, namespace, state, currentAssignments, logger, metricsScope)
	case types.LoadBalancingModeGREEDY:
		var cpuObservationState *greedy.CPUObservationState
		if len(runtimeState) > 0 && runtimeState[0] != nil {
			if runtimeState[0].GreedyCPUObservations == nil {
				runtimeState[0].GreedyCPUObservations = greedy.NewCPUObservationState()
			}
			cpuObservationState = runtimeState[0].GreedyCPUObservations
		}
		return greedy.PlanRebalance(cfg.LoadBalancingGreedy, namespace, state, currentAssignments, now, shardStatsStaleAfter, metricsScope, cpuObservationState)
	default:
		return nil, fmt.Errorf("unsupported load balancing mode: %s", mode)
	}
}
