package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"

	"github.com/uber/cadence/service/sharddistributor/canary/replay"
)

func TestDependenciesAreSatisfied(t *testing.T) {
	assert.NoError(t, fx.ValidateApp(opts(
		defaultFixedNamespace,
		defaultEphemeralNamespace,
		defaultShardDistributorEndpoint,
		defaultCanaryGRPCPort,
		defaultNumExecutors,
		defaultNumExecutors,
		defaultCanaryMetricsPort,
		replay.Options{},
	)))
}
