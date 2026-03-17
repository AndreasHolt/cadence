package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReplaySpeedFromEnv(t *testing.T) {
	t.Setenv(ReplaySpeedEnv, "")
	assert.Equal(t, 1.0, ReplaySpeedFromEnv())

	t.Setenv(ReplaySpeedEnv, "not-a-number")
	assert.Equal(t, 1.0, ReplaySpeedFromEnv())

	t.Setenv(ReplaySpeedEnv, "1.0")
	assert.Equal(t, 1.0, ReplaySpeedFromEnv())

	t.Setenv(ReplaySpeedEnv, "60")
	assert.Equal(t, 60.0, ReplaySpeedFromEnv())
}

func TestScaleDurationForReplay(t *testing.T) {
	assert.Equal(t, 10*time.Second, ScaleDurationForReplay(10*time.Second, 1.0))
	assert.Equal(t, 100*time.Millisecond, ScaleDurationForReplay(10*time.Second, 100.0))
	assert.Equal(t, MinScaledDuration, ScaleDurationForReplay(500*time.Microsecond, 100.0))
}

func TestScaleDurationForReplayWithMin(t *testing.T) {
	assert.Equal(t, 250*time.Millisecond, ScaleDurationForReplayWithMin(10*time.Second, 100.0, 250*time.Millisecond))
	assert.Equal(t, 100*time.Millisecond, ScaleDurationForReplayWithMin(10*time.Second, 100.0, 0))
}

func TestScaleShardDistributionForReplay(t *testing.T) {
	cfg := ShardDistribution{
		Election: Election{
			LeaderPeriod:           60 * time.Second,
			MaxRandomDelay:         1 * time.Second,
			FailedElectionCooldown: 1 * time.Second,
		},
		Process: LeaderProcess{
			Period:       1 * time.Second,
			Timeout:      1 * time.Second,
			HeartbeatTTL: 2 * time.Second,
			LoadBalance: LoadBalance{
				PerShardCooldown: 30 * time.Second,
				LoadSmoothingTau: 30 * time.Second,
			},
		},
	}

	scaled := ScaleShardDistributionForReplay(cfg, 60.0)
	assert.Equal(t, ScaleDurationForReplayWithMin(60*time.Second, 60.0, MinReplayPerShardCooldown), scaled.Election.LeaderPeriod)
	assert.Equal(t, ScaleDurationForReplayWithMin(1*time.Second, 60.0, MinReplayElectionDelay), scaled.Election.MaxRandomDelay)
	assert.Equal(t, ScaleDurationForReplayWithMin(1*time.Second, 60.0, MinReplayElectionDelay), scaled.Election.FailedElectionCooldown)
	assert.Equal(t, ScaleDurationForReplayWithMin(1*time.Second, 60.0, MinReplayProcessPeriod), scaled.Process.Period)
	assert.Equal(t, 1*time.Second, scaled.Process.Timeout)
	assert.Equal(t, ScaleDurationForReplayWithMin(2*time.Second, 60.0, MinReplayHeartbeatTTL), scaled.Process.HeartbeatTTL)
	assert.Equal(t, ScaleDurationForReplayWithMin(30*time.Second, 60.0, MinReplayPerShardCooldown), scaled.Process.LoadBalance.PerShardCooldown)
	assert.Equal(t, ScaleDurationForReplayWithMin(30*time.Second, 60.0, MinReplayLoadSmoothingTau), scaled.Process.LoadBalance.LoadSmoothingTau)
}
