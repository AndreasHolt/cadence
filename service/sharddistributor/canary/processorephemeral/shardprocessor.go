package processorephemeral

import (
	"context"
	"hash/fnv"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
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
	// Decide shard lifetime weight (heavy or light) deterministically
	weight := 1.0
	if ephHeavyProb > 0 {
		h := fnv.New32a()
		_, _ = h.Write([]byte(shardID))
		v := float64(h.Sum32()) / float64(math.MaxUint32)
		if v < ephHeavyProb {
			weight = ephHeavyMultiplier
		}
	}
	return &ShardProcessor{
		shardID:    shardID,
		timeSource: timeSource,
		logger:     logger,
		stopChan:   make(chan struct{}),
		status:     types.ShardStatusREADY,
		weight:     weight,
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

	status types.ShardStatus
	weight float64
}

var _ executorclient.ShardProcessor = (*ShardProcessor)(nil)

// GetShardReport implements executorclient.ShardProcessor.
func (p *ShardProcessor) GetShardReport() executorclient.ShardReport {
	return executorclient.ShardReport{
		ShardLoad: computeEphemeralLoad(p.shardID, p.weight, p.timeSource.Now()), // We return 1.0 for all shards for now.
		Status:    p.status,                                                      // Report the status of the shard
	}
}

// Start implements executorclient.ShardProcessor.
func (p *ShardProcessor) Start(ctx context.Context) error {
	p.logger.Info("Starting shard processor", zap.String("shardID", p.shardID))
	p.goRoutineWg.Add(1)
	go p.process(ctx)
	return nil
}

// Stop implements executorclient.ShardProcessor.
func (p *ShardProcessor) Stop() {
	close(p.stopChan)
	p.goRoutineWg.Wait()
}

func (p *ShardProcessor) process(ctx context.Context) {
	defer p.goRoutineWg.Done()

	ticker := p.timeSource.NewTicker(processInterval)
	defer ticker.Stop()

	stopTicker := p.timeSource.NewTicker(stopInterval)
	defer stopTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopChan:
			p.logger.Info("Stopping shard processor", zap.String("shardID", p.shardID), zap.Int("steps", p.processSteps), zap.String("status", p.status.String()))
			return
		case <-ticker.Chan():
			p.logger.Info("Processing shard", zap.String("shardID", p.shardID), zap.Int("steps", p.processSteps), zap.String("status", p.status.String()))
		case <-stopTicker.Chan():
			p.processSteps++
			if rand.Intn(shardProcessorDoneChance) == 0 {
				p.logger.Info("Setting shard processor to done", zap.String("shardID", p.shardID), zap.Int("steps", p.processSteps), zap.String("status", p.status.String()))
				p.status = types.ShardStatusDONE
			}
		}
	}
}

// Our new synthetic load model (for dynamic hot shard simulation) //

var (
	// chance shardisheavy for its lifetime
	ephHeavyProb = parseEnvFloatE("SD_EPH_HEAVY_PROB", 0.1) // chance shard is heavy for its lifetime
	// if it's 'heavy', we multiply by this. We distinguish between 'hot' for fixed (it's a temporary burst),
	// and 'heavy' for ephemeral, since it's tied to its lifetime
	ephHeavyMultiplier = parseEnvFloatE("SD_EPH_HEAVY_MULTIPLIER", 6.0)
	// Adds a small jitter to the reported load to not have it be perfectly flat (e.g., 0.1 = +-10% multiplicative noise )
	// ... this is mainly for the shards that are not marked as 'hot', to have them vary a little bit
	ephNoisePct = parseEnvFloatE("SD_EPH_LOAD_NOISE_PCT", 0.1)
	// A per-process scale factor to emulate different host capacities/compute
	// ... and in the future look into gRPC ORCA, to not having to emulate it
	ephExecScale = parseEnvFloatE("SD_EXEC_LOAD_SCALE", 1.0)
)

func computeEphemeralLoad(shardID string, weight float64, now time.Time) float64 {
	base := weight
	if ephNoisePct > 0 {
		h := fnv.New32a()
		_, _ = h.Write([]byte(shardID))
		_, _ = h.Write([]byte(strconv.FormatInt(now.Unix()/10, 10))) // change slowly
		v := float64(h.Sum32())/float64(math.MaxUint32) - 0.5
		base *= 1.0 + (2.0 * ephNoisePct * v)
	}
	if base < 0 {
		base = 0
	}
	return base * ephExecScale
}

func parseEnvFloatE(key string, def float64) float64 {
	if s := os.Getenv(key); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v
		}
	}
	return def
}
