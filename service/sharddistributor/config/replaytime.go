package config

import (
	"os"
	"strconv"
	"time"
)

const (
	// ReplaySpeedEnv is the optional env var used to speed up shard-distributor wall-clock timers
	// during replay/simulation runs.
	ReplaySpeedEnv = "SHARD_DISTRIBUTOR_REPLAY_SPEED"

	// MinScaledDuration prevents scaled positive durations from collapsing to zero.
	MinScaledDuration = time.Millisecond

	// DefaultLoadSmoothingTau is the default EWMA smoothing constant for shard load statistics.
	DefaultLoadSmoothingTau = 30 * time.Second

	// Replay-time lower bounds keep control-loop timers from becoming too aggressive for
	// local development environments at high replay speeds.
	// Floors are intentionally tuned to allow faithful 20x replay timing
	// (1s -> 50ms, 2s -> 100ms) while still preventing pathological timer collapse.
	MinReplayHeartbeatInterval = 50 * time.Millisecond
	MinReplayProcessPeriod     = 50 * time.Millisecond
	MinReplayHeartbeatTTL      = 100 * time.Millisecond
	MinReplayElectionDelay     = 50 * time.Millisecond
	MinReplayPerShardCooldown  = 1 * time.Second
	MinReplayLoadSmoothingTau  = 500 * time.Millisecond
)

// ReplaySpeedFromEnv returns the configured replay speed multiplier.
// Values <= 1.0, unset values, and parse errors all return 1.0 (disabled).
func ReplaySpeedFromEnv() float64 {
	raw, ok := os.LookupEnv(ReplaySpeedEnv)
	if !ok || raw == "" {
		return 1.0
	}

	speed, err := strconv.ParseFloat(raw, 64)
	if err != nil || speed <= 1.0 {
		return 1.0
	}
	return speed
}

// ScaleDurationForReplay scales a duration down by replay speed.
// Positive durations are clamped to at least MinScaledDuration.
func ScaleDurationForReplay(duration time.Duration, speed float64) time.Duration {
	return ScaleDurationForReplayWithMin(duration, speed, MinScaledDuration)
}

// ScaleDurationForReplayWithMin scales a duration down by replay speed with a caller-provided floor.
func ScaleDurationForReplayWithMin(duration time.Duration, speed float64, min time.Duration) time.Duration {
	if duration <= 0 || speed <= 1.0 {
		return duration
	}

	scaled := time.Duration(float64(duration) / speed)
	if min > 0 && scaled < min {
		return min
	}
	return scaled
}

// ScaleShardDistributionForReplay returns a copy of cfg with replay-sensitive timers scaled.
func ScaleShardDistributionForReplay(cfg ShardDistribution, speed float64) ShardDistribution {
	if speed <= 1.0 {
		return cfg
	}

	if cfg.Process.LoadBalance.LoadSmoothingTau <= 0 {
		cfg.Process.LoadBalance.LoadSmoothingTau = DefaultLoadSmoothingTau
	}

	cfg.Election.LeaderPeriod = ScaleDurationForReplayWithMin(cfg.Election.LeaderPeriod, speed, MinReplayPerShardCooldown)
	cfg.Election.MaxRandomDelay = ScaleDurationForReplayWithMin(cfg.Election.MaxRandomDelay, speed, MinReplayElectionDelay)
	cfg.Election.FailedElectionCooldown = ScaleDurationForReplayWithMin(cfg.Election.FailedElectionCooldown, speed, MinReplayElectionDelay)

	cfg.Process.Period = ScaleDurationForReplayWithMin(cfg.Process.Period, speed, MinReplayProcessPeriod)
	// Keep per-cycle timeout unchanged. This bounds etcd round-trip failures under high replay speeds.
	cfg.Process.HeartbeatTTL = ScaleDurationForReplayWithMin(cfg.Process.HeartbeatTTL, speed, MinReplayHeartbeatTTL)
	cfg.Process.LoadBalance.PerShardCooldown = ScaleDurationForReplayWithMin(cfg.Process.LoadBalance.PerShardCooldown, speed, MinReplayPerShardCooldown)
	cfg.Process.LoadBalance.LoadSmoothingTau = ScaleDurationForReplayWithMin(cfg.Process.LoadBalance.LoadSmoothingTau, speed, MinReplayLoadSmoothingTau)

	return cfg
}
