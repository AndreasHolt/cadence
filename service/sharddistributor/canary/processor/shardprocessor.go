package processor

import (
	"context"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"go.uber.org/zap"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/canary/replay"
	"github.com/uber/cadence/service/sharddistributor/client/executorclient"
)

// This is a small shard processor, the only thing it currently does it
// count the number of steps it has processed and log that information.
const (
	processInterval = 10 * time.Second
)

// NewShardProcessor creates a new ShardProcessor.
func NewShardProcessor(shardID string, timeSource clock.TimeSource, logger *zap.Logger) *ShardProcessor {
	p := &ShardProcessor{
		shardID:    shardID,
		timeSource: timeSource,
		logger:     logger,
		stopChan:   make(chan struct{}),
	}
	p.status.Store(int32(types.ShardStatusREADY))
	return p
}

// NewShardProcessorConstructor returns a shard processor constructor that reports replayed shard loads.
func NewShardProcessorConstructor(loadProvider replay.LoadProvider) func(string, clock.TimeSource, *zap.Logger) *ShardProcessor {
	return func(shardID string, timeSource clock.TimeSource, logger *zap.Logger) *ShardProcessor {
		p := NewShardProcessor(shardID, timeSource, logger)
		p.loadProvider = loadProvider
		return p
	}
}

// ShardProcessor is a processor for a shard.
type ShardProcessor struct {
	shardID      string
	timeSource   clock.TimeSource
	logger       *zap.Logger
	stopChan     chan struct{}
	goRoutineWg  sync.WaitGroup
	processSteps int

	status       atomic.Int32
	loadProvider replay.LoadProvider
}

var _ executorclient.ShardProcessor = (*ShardProcessor)(nil)

// GetShardReport implements executorclient.ShardProcessor.
func (p *ShardProcessor) GetShardReport() executorclient.ShardReport {
	load := 1.0
	if p.loadProvider != nil {
		if v, ok := p.loadProvider.LoadForShard(p.shardID); ok {
			load = v
		} else {
			load = 0
		}
	} else {
		load = p.shardLoad // We get a load from shardID
	}
	return executorclient.ShardReport{
		ShardLoad: load,
		Status:    types.ShardStatus(p.status.Load()), // Report the shard as ready since it's actively processing
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
	p.logger.Info("Stopping shard processor", zap.String("shardID", p.shardID))
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

	for {
		select {
		case <-p.stopChan:
			p.logger.Info("Stopping shard processor", zap.String("shardID", p.shardID), zap.Int("steps", p.processSteps))
			return
		case <-ticker.Chan():
			p.processSteps++
			p.logger.Info("Processing shard", zap.String("shardID", p.shardID), zap.Int("steps", p.processSteps))
		}
	}
}

func (p *ShardProcessor) calculateLoad() float64 {

	hotFraction := getEnvFloat("SD_HOT_FRACTION", 0.05)
	hotMultiplier := getEnvFloat("SD_HOT_MULTIPLIER", 8.0)
	rotateSeconds := getEnvFloat("SD_HOT_ROTATE_SECONDS", 120.0)
	noisePct := getEnvFloat("SD_LOAD_NOISE_PCT", 0.10)
	execScale := getEnvFloat("SD_EXEC_LOAD_SCALE", 1.0)

	load := 1.0

	// Determine if shard is hot based on ID and time rotation
	period := int64(0)
	if rotateSeconds > 0 {
		period = p.timeSource.Now().Unix() / int64(rotateSeconds)
	}

	// Use a hash of shardID and period to decide hotness
	h := farm.Fingerprint64([]byte(p.shardID + strconv.FormatInt(period, 10)))
	if float64(h%1000)/1000.0 < hotFraction {
		load *= hotMultiplier
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
