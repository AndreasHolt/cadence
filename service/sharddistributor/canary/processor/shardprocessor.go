package processor

import (
	"context"
	"hash/fnv"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/executorclient"
)

// This is a small shard processor, the only thing it currently does it
// count the number of steps it has processed and log that information.
const (
	processInterval = 10 * time.Second
)

// NewShardProcessor creates a new ShardProcessor.
func NewShardProcessor(shardID string, timeSource clock.TimeSource, logger *zap.Logger) *ShardProcessor {
	return &ShardProcessor{
		shardID:    shardID,
		timeSource: timeSource,
		logger:     logger,
		stopChan:   make(chan struct{}),
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
}

var _ executorclient.ShardProcessor = (*ShardProcessor)(nil)

// GetShardReport implements executorclient.ShardProcessor.
func (p *ShardProcessor) GetShardReport() executorclient.ShardReport {
	// Report synthetic load. Defaults to 1.0 (like legacyimpl.), but can simulate hot shards via env knobs.
	load := computeSyntheticLoad(p.shardID, p.timeSource.Now())
	return executorclient.ShardReport{
		ShardLoad: load,
		Status:    types.ShardStatusREADY,
	}
}

// Start implements executorclient.ShardProcessor.
func (p *ShardProcessor) Start(ctx context.Context) {
	p.logger.Info("Starting shard processor", zap.String("shardID", p.shardID))
	p.goRoutineWg.Add(1)
	go p.process(ctx)
}

// Our new synthetic load model (for dynamic hot shard simulation) //

var (
	// Fraction of shards considered "hot" in a time bucket (0..1, where the default 0 means hot shards are disabled)
	envHotFraction = parseEnvFloat("SD_HOT_FRACTION", 0)
	// Load multiplier for hot shards (default 8.0)
	envHotMultiplier = parseEnvFloat("SD_HOT_MULTIPLIER", 8.0)
	// Duration of a hot bucket lasts, before a new set of hot shards is found. hot set rotates every SD_HOT_ROTATE_SECONDS (default 0 = disabled)
	envRotateSeconds = parseEnvInt("SD_HOT_ROTATE_SECONDS", 0)
	// Adds a small jitter to the reported load to not have it be perfectly flat (e.g., 0.1 = +-10% multiplicative noise )
	// ... this is mainly for the shards that are not marked as 'hot', to have them vary a little bit
	envNoisePct = parseEnvFloat("SD_LOAD_NOISE_PCT", 0)
	// A per-process scale factor to emulate different host capacities/compute 
	// ... and in the future look into gRPC ORCA, to not having to emulate it
	envExecLoadScale = parseEnvFloat("SD_EXEC_LOAD_SCALE", 1.0)
)

func computeSyntheticLoad(shardID string, now time.Time) float64 {
	// If disabled = original constant load
	if envHotFraction <= 0 || envRotateSeconds <= 0 {
		return 1.0 * envExecLoadScale
	}
	bucket := now.Unix() / int64(envRotateSeconds)
	// Hash shardID+bucket to decide hot membership deterministically for the bucket
	// ... a bucket is the time window / grouping unit (every envRotateSeconds)
	// ... and only shards inside those hot buckets can be multiplied with the envHotMultiplier
	h := fnv.New32a() 
	_, _ = h.Write([]byte(shardID)) // Add shard ID to hash
	_, _ = h.Write([]byte(":"))
	_, _ = h.Write([]byte(strconv.FormatInt(bucket, 10))) // Convert integer bucket into string form and add to hash
	v := float64(h.Sum32()) / float64(math.MaxUint32) // Return uint32 from from hash state, and divide by max uint32 to get num in range [0, 1)
	hot := v < envHotFraction // Determine if we are marked as 'hot' 
	base := 1.0
	if hot {
		base = envHotMultiplier
	}
	// Always add small multiplicative noise to have varying load
	if envNoisePct > 0 {
		h2 := fnv.New32a()
		_, _ = h2.Write([]byte(shardID))
		_, _ = h2.Write([]byte("#"))
		_, _ = h2.Write([]byte(strconv.FormatInt(bucket, 10)))
		v2 := float64(h2.Sum32())/float64(math.MaxUint32) - 0.5 // [-0.5, 0.5]
		base *= 1.0 + (2.0 * envNoisePct * v2)
	}
	if base < 0.0 {
		base = 0.0
	}
	return base * envExecLoadScale
}

func parseEnvFloat(key string, def float64) float64 {
	if s := os.Getenv(key); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v
		}
	}
	return def
}

func parseEnvInt(key string, def int) int {
	if s := os.Getenv(key); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}

// Stop implements executorclient.ShardProcessor.
func (p *ShardProcessor) Stop() {
	p.logger.Info("Stopping shard processor", zap.String("shardID", p.shardID))
	close(p.stopChan)
	p.goRoutineWg.Wait()
}

func (p *ShardProcessor) process(ctx context.Context) {
	defer p.goRoutineWg.Done()

	ticker := p.timeSource.NewTicker(processInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopChan:
			return
		case <-ticker.Chan():
			p.processSteps++
			p.logger.Info("Processing shard", zap.String("shardID", p.shardID), zap.Int("steps", p.processSteps))
		}
	}
}
