package processorephemeral

import (
	"context"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/client/executorclient"
)

// This is a small shard processor, the only thing it currently does it
// count the number of steps it has processed and log that information.
const (
	processInterval = 10 * time.Second

	// We create a new shard every second. For each of them we have a chance of them to be done of 1/60 every second.
	// This means the average time to complete a shard is 60 seconds.
	// It also means we in average have 60 shards per instance running at any given time.
	stopInterval             = 1 * time.Second
	shardProcessorDoneChance = 60
)

// NewShardProcessor creates a new ShardProcessor.
func NewShardProcessor(shardID string, timeSource clock.TimeSource, logger *zap.Logger) *ShardProcessor {
	p := &ShardProcessor{
		shardID:    shardID,
		timeSource: timeSource,
		logger:     logger,
		stopChan:   make(chan struct{}),
	}

	// Decide if this ephemeral shard is "heavy"
	heavyProb := getEnvFloat("SD_EPH_HEAVY_PROB", 0.20)
	if rand.Float64() < heavyProb {
		p.isHeavy = true
	}

	p.SetShardStatus(types.ShardStatusREADY)
	return p
}

// ShardProcessor is a processor for a shard.
type ShardProcessor struct {
	shardID      string
	timeSource   clock.TimeSource
	logger       *zap.Logger
	stopChan     chan struct{}
	goRoutineWg  sync.WaitGroup
	processSteps int
	isHeavy      bool

	status atomic.Int32
}

var _ executorclient.ShardProcessor = (*ShardProcessor)(nil)

// GetShardReport implements executorclient.ShardProcessor.
func (p *ShardProcessor) GetShardReport() executorclient.ShardReport {
	return executorclient.ShardReport{
		ShardLoad: p.calculateLoad(),                  // We return a simulated load
		Status:    types.ShardStatus(p.status.Load()), // Report the status of the shard
	}
}

// Start implements executorclient.ShardProcessor.
func (p *ShardProcessor) Start(_ context.Context) error {
	p.logger.Info("Starting shard processor", zap.String("shardID", p.shardID))
	p.goRoutineWg.Add(1)
	go p.process()
	return nil
}

// Stop implements executorclient.ShardProcessor.
func (p *ShardProcessor) Stop() {
	close(p.stopChan)
	p.goRoutineWg.Wait()
}

func (p *ShardProcessor) SetShardStatus(status types.ShardStatus) {
	p.status.Store(int32(status))
}

func (p *ShardProcessor) process() {
	defer p.goRoutineWg.Done()

	ticker := p.timeSource.NewTicker(processInterval)
	defer ticker.Stop()

	stopTicker := p.timeSource.NewTicker(stopInterval)
	defer stopTicker.Stop()

	for {
		select {
		case <-p.stopChan:
			p.logger.Info("Stopping shard processor", zap.String("shardID", p.shardID), zap.Int("steps", p.processSteps), zap.String("status", types.ShardStatus(p.status.Load()).String()))
			return
		case <-ticker.Chan():
			p.logger.Info("Processing shard", zap.String("shardID", p.shardID), zap.Int("steps", p.processSteps), zap.String("status", types.ShardStatus(p.status.Load()).String()))
		case <-stopTicker.Chan():
			p.processSteps++
			if rand.Intn(shardProcessorDoneChance) == 0 {
				p.logger.Info("Setting shard processor to done", zap.String("shardID", p.shardID), zap.Int("steps", p.processSteps), zap.String("status", types.ShardStatus(p.status.Load()).String()))
				p.SetShardStatus(types.ShardStatusDONE)
			}
		}
	}
}

func (p *ShardProcessor) calculateLoad() float64 {

	heavyMultiplier := getEnvFloat("SD_EPH_HEAVY_MULTIPLIER", 6.0)
	noisePct := getEnvFloat("SD_EPH_LOAD_NOISE_PCT", 0.10)
	execScale := getEnvFloat("SD_EXEC_LOAD_SCALE", 1.0)

	load := 1.0
	if p.isHeavy {
		load *= heavyMultiplier
	}

	// Add noise
	if noisePct > 0 {
		noise := (rand.Float64()*2 - 1) * noisePct
		load *= (1 + noise)
	}

	// Apply executor scale
	load *= execScale

	return math.Max(0.1, load)
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultValue
}
