package tasklist

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

func mustNewIdentifier(domainID, name string, taskType int) *Identifier {
	id, err := NewIdentifier(domainID, name, taskType)
	if err != nil {
		panic(err)
	}
	return id
}

var testIdentifier = mustNewIdentifier("domain-id", "tl", persistence.TaskListTypeDecision)

type shardProcessorTestData struct {
	mockRegistry   *MockTaskListRegistry
	shardProcessor ShardProcessor
}

type shardLoadManager struct {
	*MockManager
	load float64
}

func (m *shardLoadManager) shardLoad() float64 {
	return m.load
}

func newShardProcessorTestData(t *testing.T, taskListID *Identifier) shardProcessorTestData {
	ctrl := gomock.NewController(t)

	mockRegistry := NewMockTaskListRegistry(ctrl)
	mockRegistry.EXPECT().AllManagers().Return([]Manager{}).AnyTimes()

	params := ShardProcessorParams{
		ShardID:           taskListID.GetName(),
		TaskListsRegistry: mockRegistry,
		ReportTTL:         1 * time.Millisecond,
		TimeSource:        clock.NewRealTimeSource(),
	}

	shardProcessor, err := NewShardProcessor(params)
	require.NoError(t, err)
	return shardProcessorTestData{
		mockRegistry:   mockRegistry,
		shardProcessor: shardProcessor,
	}
}

func TestNewShardProcessorFailsWithEmptyParams(t *testing.T) {
	params := ShardProcessorParams{}
	sp, err := NewShardProcessor(params)
	require.Nil(t, sp)
	require.Error(t, err)
}

func TestGetShardReport(t *testing.T) {
	td := newShardProcessorTestData(t, testIdentifier)

	shardReport := td.shardProcessor.GetShardReport()
	require.NotNil(t, shardReport)
	require.Equal(t, float64(0), shardReport.ShardLoad)
	require.Equal(t, types.ShardStatusREADY, shardReport.Status)
}

func TestSetShardStatus(t *testing.T) {
	defer goleak.VerifyNone(t)
	td := newShardProcessorTestData(t, testIdentifier)

	td.shardProcessor.SetShardStatus(types.ShardStatusREADY)
	shardReport := td.shardProcessor.GetShardReport()
	require.NotNil(t, shardReport)
	require.Equal(t, float64(0), shardReport.ShardLoad)
	require.Equal(t, types.ShardStatusREADY, shardReport.Status)
}

func TestGetShardReportUsesManagerShardLoad(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := NewMockTaskListRegistry(ctrl)
	mockManager := NewMockManager(ctrl)
	manager := &shardLoadManager{MockManager: mockManager, load: 7.5}
	mockManager.EXPECT().TaskListID().Return(testIdentifier).AnyTimes()

	mockRegistry.EXPECT().AllManagers().Return([]Manager{manager}).AnyTimes()

	params := ShardProcessorParams{
		ShardID:           testIdentifier.GetName(),
		TaskListsRegistry: mockRegistry,
		ReportTTL:         0,
		TimeSource:        clock.NewRealTimeSource(),
	}

	shardProcessor, err := NewShardProcessor(params)
	require.NoError(t, err)

	shardReport := shardProcessor.GetShardReport()
	require.Equal(t, 7.5, shardReport.ShardLoad)
}

func TestGetShardReportAggregatesPartitionedManagersByRoot(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := NewMockTaskListRegistry(ctrl)

	rootID := mustNewIdentifier("domain-id", "3", persistence.TaskListTypeDecision)
	partitionID := mustNewIdentifier("domain-id", "/__cadence_sys/3/1", persistence.TaskListTypeDecision)
	otherID := mustNewIdentifier("domain-id", "4", persistence.TaskListTypeDecision)

	rootMgr := &shardLoadManager{MockManager: NewMockManager(ctrl), load: 2.0}
	rootMgr.MockManager.EXPECT().TaskListID().Return(rootID).AnyTimes()

	partitionMgr := &shardLoadManager{MockManager: NewMockManager(ctrl), load: 5.0}
	partitionMgr.MockManager.EXPECT().TaskListID().Return(partitionID).AnyTimes()

	otherMgr := &shardLoadManager{MockManager: NewMockManager(ctrl), load: 11.0}
	otherMgr.MockManager.EXPECT().TaskListID().Return(otherID).AnyTimes()

	mockRegistry.EXPECT().AllManagers().Return([]Manager{rootMgr, partitionMgr, otherMgr}).AnyTimes()

	params := ShardProcessorParams{
		ShardID:           "3",
		TaskListsRegistry: mockRegistry,
		ReportTTL:         0,
		TimeSource:        clock.NewRealTimeSource(),
	}

	shardProcessor, err := NewShardProcessor(params)
	require.NoError(t, err)

	shardReport := shardProcessor.GetShardReport()
	require.Equal(t, 7.0, shardReport.ShardLoad)
}
