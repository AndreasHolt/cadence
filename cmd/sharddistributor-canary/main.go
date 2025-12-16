package main

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber-go/tally/prometheus"
	"github.com/urfave/cli/v2"
	"go.uber.org/fx"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/peer"
	"go.uber.org/yarpc/transport/grpc"
	"go.uber.org/zap"

	sharddistributorv1 "github.com/uber/cadence/.gen/proto/sharddistributor/v1"
	"github.com/uber/cadence/common/clock"
	cadenceconfig "github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/service/sharddistributor/canary"
	canaryConfig "github.com/uber/cadence/service/sharddistributor/canary/config"
	"github.com/uber/cadence/service/sharddistributor/canary/executors"
	"github.com/uber/cadence/service/sharddistributor/client/clientcommon"
	"github.com/uber/cadence/service/sharddistributor/client/executorclient"
	"github.com/uber/cadence/service/sharddistributor/client/spectatorclient"
	sdconfig "github.com/uber/cadence/service/sharddistributor/config"
	"github.com/uber/cadence/tools/common/commoncli"
)

const (
	// Default configuration
	defaultShardDistributorEndpoint = "127.0.0.1:7943"
	defaultFixedNamespace           = "shard-distributor-canary"
	defaultEphemeralNamespace       = "shard-distributor-canary-ephemeral"
	defaultCanaryGRPCPort           = 7953 // Port for canary to receive ping requests
	defaultNumExecutors             = 1
	defaultCanaryMetricsPort        = 9098
	defaultReplayNamespace          = "shard-distributor-replay"

	shardDistributorServiceName = "cadence-shard-distributor"
)

func runApp(c *cli.Context) {
	endpoint := c.String("endpoint")
	fixedNamespace := c.String("fixed-namespace")
	ephemeralNamespace := c.String("ephemeral-namespace")
	canaryGRPCPort := c.Int("canary-grpc-port")

	numFixedExecutors := c.Int("num-fixed-executors")
	numEphemeralExecutors := c.Int("num-ephemeral-executors")

	if c.IsSet("num-executors") {
		numExecutors := c.Int("num-executors")
		numFixedExecutors = numExecutors
		numEphemeralExecutors = numExecutors

	}

	replayOpts := replay.Options{
		CSVPath:           c.String("replay-csv"),
		Speed:             c.Float64("replay-speed"),
		Namespace:         c.String("replay-namespace"),
		NumFixedExecutors: c.Int("replay-num-fixed-executors"),
	}

	fx.New(opts(fixedNamespace, ephemeralNamespace, endpoint, canaryGRPCPort, numFixedExecutors, numEphemeralExecutors, canaryMetricsPort, replayOpts)).Run()
}

func opts(fixedNamespace, ephemeralNamespace, endpoint string, canaryGRPCPort int, numFixedExecutors, numEphemeral int, canaryMetricsPort int, replayOpts replay.Options) fx.Option {

	logger, _ := zap.NewDevelopment()
	cadenceLogger := log.NewLogger(logger)

	metricsConfig := cadenceconfig.Metrics{
		Prometheus: &prometheus.Configuration{
			ListenAddress: "127.0.0.1:9098",
			TimerType:     "histogram",
		},
	}
	metricsScope := metricsConfig.NewScope(cadenceLogger, "shard-distributor-canary")

	configuration := clientcommon.Config{
		Namespaces: []clientcommon.NamespaceConfig{
			{Namespace: fixedNamespace, HeartBeatInterval: 1 * time.Second, MigrationMode: sdconfig.MigrationModeONBOARDED},
			{Namespace: ephemeralNamespace, HeartBeatInterval: 1 * time.Second, MigrationMode: sdconfig.MigrationModeONBOARDED},
			{Namespace: executors.LocalPassthroughNamespace, HeartBeatInterval: 1 * time.Second, MigrationMode: sdconfig.MigrationModeLOCALPASSTHROUGH},
			{Namespace: executors.LocalPassthroughShadowNamespace, HeartBeatInterval: 1 * time.Second, MigrationMode: sdconfig.MigrationModeLOCALPASSTHROUGHSHADOW},
			{Namespace: executors.DistributedPassthroughNamespace, HeartBeatInterval: 1 * time.Second, MigrationMode: sdconfig.MigrationModeDISTRIBUTEDPASSTHROUGH},
			{Namespace: executors.ExternalAssignmentNamespace, HeartBeatInterval: 1 * time.Second, MigrationMode: sdconfig.MigrationModeDISTRIBUTEDPASSTHROUGH},
		},
	}

	canaryGRPCAddress := fmt.Sprintf("127.0.0.1:%d", canaryGRPCPort)

	// Create listener for GRPC inbound
	listener, err := net.Listen("tcp", canaryGRPCAddress)
	if err != nil {
		panic(err)
	}

	transport := grpc.NewTransport()

	executorMetadata := executorclient.ExecutorMetadata{
		clientcommon.GrpcAddressMetadataKey: canaryGRPCAddress,
	}

	return fx.Options(
		fx.Supply(
			fx.Annotate(metricsScope, fx.As(new(tally.Scope))),
			fx.Annotate(clock.NewRealTimeSource(), fx.As(new(clock.TimeSource))),
			configuration,
			transport,
			executorMetadata,
		),

		fx.Provide(func(peerChooser spectatorclient.SpectatorPeerChooserInterface) yarpc.Config {
			return yarpc.Config{
				Name: "shard-distributor-canary",
				Inbounds: yarpc.Inbounds{
					transport.NewInbound(listener), // Listen for incoming ping requests
				},
				Outbounds: yarpc.Outbounds{
					shardDistributorServiceName: {
						Unary:  transport.NewSingleOutbound(endpoint),
						Stream: transport.NewSingleOutbound(endpoint),
					},
					// canary-to-canary outbound uses peer chooser to route to other canary instances
					"shard-distributor-canary": {
						Unary:  transport.NewOutbound(peerChooser),
						Stream: transport.NewOutbound(peerChooser),
					},
				},
			}
		}),

		fx.Provide(
			func(t *grpc.Transport) peer.Transport { return t },
		),
		fx.Provide(
			yarpc.NewDispatcher,
			func(d *yarpc.Dispatcher) yarpc.ClientConfig { return d }, // Reprovide the dispatcher as a client config
			func(l *zap.Logger) log.Logger { return log.NewLogger(l) },
		),

		// We do decorate instead of Invoke because we want to start and stop the dispatcher at the
		// correct time.
		// It will start before all dependencies are started and stop after all dependencies are stopped.
		// The Decorate gives fx enough information, so it can start and stop the dispatcher at the correct time.
		//
		// It is critical to start and stop the dispatcher at the correct time.
		// Since the executors need to
		// be able to send a final "drain" request to the shard distributor before the application is stopped.
		fx.Decorate(func(
			lc fx.Lifecycle,
			dispatcher *yarpc.Dispatcher,
			server sharddistributorv1.ShardDistributorExecutorCanaryAPIYARPCServer,
		) *yarpc.Dispatcher {
			// Register canary procedures and ensure dispatcher lifecycle is managed by fx.
			dispatcher.Register(sharddistributorv1.BuildShardDistributorExecutorCanaryAPIYARPCProcedures(server))
			lc.Append(fx.StartStopHook(dispatcher.Start, dispatcher.Stop))
			return dispatcher
		}),

		// Include the canary module - it will set up spectator peer choosers and canary client
		canary.Module(canary.NamespacesNames{
			FixedNamespace:              fixedNamespace,
			EphemeralNamespace:          ephemeralNamespace,
			ExternalAssignmentNamespace: executors.ExternalAssignmentNamespace,
			SharddistributorServiceName: shardDistributorServiceName,
			Config: canaryConfig.Config{
				Canary: canaryConfig.CanaryConfig{
					NumFixedExecutors:     numFixedExecutors,
					NumEphemeralExecutors: numEphemeral,
				},
			},
		}),
		canary.ModuleWithReplay(
			canary.NamespacesNames{
				FixedNamespace:              fixedNamespace,
				EphemeralNamespace:          ephemeralNamespace,
				ExternalAssignmentNamespace: executors.ExternalAssignmentNamespace,
				SharddistributorServiceName: shardDistributorServiceName,
			},
			replayOpts,
		),
	)
}

func buildCLI() *cli.App {
	app := cli.NewApp()
	app.Name = "sharddistributor-canary"
	app.Usage = "Cadence shard distributor canary"
	app.Version = "0.0.1"

	app.Commands = []*cli.Command{
		{
			Name:  "start",
			Usage: "start shard distributor canary",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "endpoint",
					Aliases: []string{"e"},
					Value:   defaultShardDistributorEndpoint,
					Usage:   "shard distributor endpoint address",
				},
				&cli.StringFlag{
					Name:  "fixed-namespace",
					Value: defaultFixedNamespace,
					Usage: "namespace for fixed shard processing",
				},
				&cli.StringFlag{
					Name:  "ephemeral-namespace",
					Value: defaultEphemeralNamespace,
					Usage: "namespace for ephemeral shard creation testing",
				},
				&cli.IntFlag{
					Name:  "canary-grpc-port",
					Value: defaultCanaryGRPCPort,
					Usage: "port for canary to receive ping requests",
				},
				&cli.IntFlag{
					Name:  "num-executors",
					Value: defaultNumExecutors,
					Usage: "number of executors for fixed and ephemeral to start. Overrides num-fixed-executors and num-ephemeral-executors flags",
				},
				&cli.IntFlag{
					Name:  "num-fixed-executors",
					Value: defaultNumExecutors,
					Usage: "number of executors of fixed namespace to start. Don't use with num-executors",
				},
				&cli.IntFlag{
					Name:  "num-ephemeral-executors",
					Value: defaultNumExecutors,
					Usage: "number of executors of ephemeral namespace to start. Don't use with num-executors",
					Name:  "canary-metrics-port",
					Value: defaultCanaryMetricsPort,
					Usage: "port for canary Prometheus metrics",
				},
				&cli.StringFlag{
					Name:  "replay-csv",
					Usage: "enable CSV load replay (path to CSV: timestamp,load0,...,loadN-1)",
				},
				&cli.StringFlag{
					Name:  "replay-namespace",
					Value: defaultReplayNamespace,
					Usage: "fixed namespace used for CSV replay",
				},
				&cli.IntFlag{
					Name:  "replay-num-fixed-executors",
					Value: 3,
					Usage: "number of fixed-namespace executors to run in-process during replay",
				},
				&cli.Float64Flag{
					Name:  "replay-speed",
					Value: 1.0,
					Usage: "CSV replay speed multiplier (timestamp-following mode)",
				},
			},
			Action: func(c *cli.Context) error {
				runApp(c)
				return nil
			},
		},
	}

	return app
}

func main() {
	app := buildCLI()
	commoncli.ExitHandler(app.Run(os.Args))
}
