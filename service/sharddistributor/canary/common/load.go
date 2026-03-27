package common

import (
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/dgryski/go-farm"
)

func GetHotLoadEnabled(envPrefix string) bool {
	return os.Getenv(envPrefix+"_HOT_FRACTION") != "" ||
		os.Getenv(envPrefix+"_HOT_MULTIPLIER") != "" ||
		os.Getenv(envPrefix+"_HOT_ROTATE_SECONDS") != "" ||
		os.Getenv(envPrefix+"_LOAD_NOISE_PCT") != "" ||
		os.Getenv("SD_EXEC_LOAD_SCALE") != ""
}

func GetEphemeralHotLoadEnabled() bool {
	return os.Getenv("SD_EPH_HEAVY_PROB") != "" ||
		os.Getenv("SD_EPH_HEAVY_MULTIPLIER") != "" ||
		os.Getenv("SD_EPH_LOAD_NOISE_PCT") != "" ||
		os.Getenv("SD_EXEC_LOAD_SCALE") != ""
}

func ShardLoadFromID(shardID string) float64 {
	if parsed, err := strconv.Atoi(shardID); err == nil && parsed > 0 {
		return float64(parsed)
	}
	return 1.0
}

func GetEnvFloat(key string, defaultValue float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultValue
}

func CalculateHotLoad(shardID string, timeSource time.Time, hotFraction, hotMultiplier, rotateSeconds, noisePct, execScale float64) float64 {
	load := 1.0

	period := int64(0)
	if rotateSeconds > 0 {
		period = timeSource.Unix() / int64(rotateSeconds)
	}

	h := farm.Fingerprint64([]byte(shardID + strconv.FormatInt(period, 10)))
	if float64(h%1000)/1000.0 < hotFraction {
		load *= hotMultiplier
	}

	if noisePct > 0 {
		noise := (rand.Float64()*2 - 1) * noisePct
		load *= (1 + noise)
	}

	load *= execScale

	return math.Max(0.1, load)
}
