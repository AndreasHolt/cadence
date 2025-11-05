package process

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/service/sharddistributor/store"
)

func TestPlanLoadBasedAssignment_EmptyInputs(t *testing.T) {
	result := assignUnassignedShards(nil, nil, nil, nil)
	assert.Empty(t, result)
}

func TestPlanLoadBasedAssignment_BalancesByWeight(t *testing.T) {
	stats := map[string]store.ShardStatistics{
		"sA": {SmoothedLoad: 3},
		"sB": {SmoothedLoad: 1},
	}
	loads := map[string]float64{
		"exec-1": 0,
		"exec-2": 0,
	}
	current := map[string][]string{
		"exec-1": nil,
		"exec-2": nil,
	}

	result := assignUnassignedShards([]string{"sA", "sB"}, loads, stats, current)

	assert.Len(t, result, 2)
	assert.ElementsMatch(t, []string{"sA"}, result["exec-1"])
	assert.ElementsMatch(t, []string{"sB"}, result["exec-2"])
}

func TestPlanLoadBasedAssignment_RespectsExistingLoad(t *testing.T) {
	stats := map[string]store.ShardStatistics{
		"s1": {SmoothedLoad: 2},
		"s2": {SmoothedLoad: 1},
	}
	loads := map[string]float64{
		"exec-1": 5,
		"exec-2": 1,
		"exec-3": 4,
	}
	current := map[string][]string{
		"exec-1": {"existing"},
		"exec-2": {},
		"exec-3": {"existing"},
	}

	assignments := assignUnassignedShards([]string{"s1", "s2"}, loads, stats, current)

	assert.ElementsMatch(t, []string{"s1", "s2"}, assignments["exec-2"])
	assert.Empty(t, assignments["exec-1"])
	assert.Empty(t, assignments["exec-3"])
}

func TestPlanLoadBasedAssignment_UsesCountTieBreaker(t *testing.T) {
	stats := map[string]store.ShardStatistics{
		"s1": {SmoothedLoad: 0},
		"s2": {SmoothedLoad: 0},
	}
	loads := map[string]float64{
		"exec-1": 0,
		"exec-2": 0,
	}
	current := map[string][]string{
		"exec-1": {"existing"},
		"exec-2": {},
	}

	assignments := assignUnassignedShards([]string{"s1", "s2"}, loads, stats, current)

	assert.Len(t, assignments["exec-2"], 1)
	assert.Len(t, assignments["exec-1"], 1)
}

func TestRedistributeToEmptyExecutors(t *testing.T) {
	p := &namespaceProcessor{
		timeSource: clock.NewMockedTimeSource(),
	}
	stats := map[string]store.ShardStatistics{
		"s1": {SmoothedLoad: 5, LastMoveTime: p.timeSource.Now().Unix() - 100000},
		"s2": {SmoothedLoad: 2, LastMoveTime: p.timeSource.Now().Unix() - 100000},
		"s3": {SmoothedLoad: 1, LastMoveTime: p.timeSource.Now().Unix() - 100000},
	}
	loads := map[string]float64{
		"exec-1": 7,
		"exec-2": 0,
		"exec-3": 1,
	}
	assignments := map[string][]string{
		"exec-1": {"s1", "s2"},
		"exec-2": {},
		"exec-3": {"s3"},
	}

	steals, updated := p.redistributeToEmptyExecutors(loads, stats, assignments)

	assert.ElementsMatch(t, []string{"s1"}, steals["exec-2"])
	assert.InDelta(t, 2, updated["exec-1"], 1e-9)
	assert.InDelta(t, 5, updated["exec-2"], 1e-9)
}

func TestRedistributeToEmptyExecutors_WithCooldown(t *testing.T) {
	p := &namespaceProcessor{
		timeSource: clock.NewMockedTimeSource(),
	}
	stats := map[string]store.ShardStatistics{
		"s1": {SmoothedLoad: 5, LastMoveTime: p.timeSource.Now().Unix()},
		"s2": {SmoothedLoad: 2, LastMoveTime: p.timeSource.Now().Unix() - 100000},
		"s3": {SmoothedLoad: 1, LastMoveTime: p.timeSource.Now().Unix() - 100000},
	}
	loads := map[string]float64{
		"exec-1": 7,
		"exec-2": 0,
		"exec-3": 1,
	}
	assignments := map[string][]string{
		"exec-1": {"s1", "s2"},
		"exec-2": {},
		"exec-3": {"s3"},
	}

	steals, updated := p.redistributeToEmptyExecutors(loads, stats, assignments)

	assert.ElementsMatch(t, []string{"s2"}, steals["exec-2"])
	assert.InDelta(t, 5, updated["exec-1"], 1e-9)
	assert.InDelta(t, 2, updated["exec-2"], 1e-9)
}
