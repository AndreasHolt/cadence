package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
	"go.uber.org/yarpc/transport/grpc"
	"go.uber.org/yarpc/transport/tchannel"
	"gopkg.in/yaml.v2"

	matchingv1 "github.com/uber/cadence/.gen/proto/matching/v1"
	frontendClient "github.com/uber/cadence/client/frontend"
	matchingClient "github.com/uber/cadence/client/matching"
	grpcClient "github.com/uber/cadence/client/wrappers/grpc"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/rpc"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/types"
)

type config struct {
	FrontendGRPCEndpoint string           `yaml:"frontend_grpc_endpoint"`
	MatchingGRPCEndpoint string           `yaml:"matching_grpc_endpoint"`
	MatchingPeers        []string         `yaml:"matching_peers"`
	Domain               string           `yaml:"domain"`
	RunID                string           `yaml:"run_id"`
	RetentionDays        int32            `yaml:"retention_days"`
	Duration             time.Duration    `yaml:"duration"`
	PollTimeout          time.Duration    `yaml:"poll_timeout"`
	SummaryInterval      time.Duration    `yaml:"summary_interval"`
	Generator            generatorConfig  `yaml:"generator"`
	Trace                traceConfig      `yaml:"trace"`
	TaskLists            []taskListConfig `yaml:"task_lists"`
}

type generatorConfig struct {
	RatePerSecond int `yaml:"rate_per_second"`
	Burst         int `yaml:"burst"`
}

type taskListConfig struct {
	Name           string        `yaml:"name"`
	Weight         int           `yaml:"weight"`
	Pollers        int           `yaml:"pollers"`
	ProcessTime    time.Duration `yaml:"process_time"`
	IsolationGroup string        `yaml:"isolation_group"`
}

type stats struct {
	started     atomic.Int64
	startErr    atomic.Int64
	polled      atomic.Int64
	completed   atomic.Int64
	emptyPolls  atomic.Int64
	pollErr     atomic.Int64
	completeErr atomic.Int64

	mu             sync.Mutex
	workflowStarts map[string]time.Time
	latencies      []time.Duration
}

type summarySnapshot struct {
	AtSeconds               float64 `json:"at_seconds"`
	WindowSeconds           float64 `json:"window_seconds"`
	Started                 int64   `json:"started"`
	StartErrors             int64   `json:"start_errors"`
	Polled                  int64   `json:"polled"`
	Completed               int64   `json:"completed"`
	EmptyPolls              int64   `json:"empty_polls"`
	PollErrors              int64   `json:"poll_errors"`
	CompletionErrors        int64   `json:"completion_errors"`
	WindowStarted           int64   `json:"window_started"`
	WindowStartErrors       int64   `json:"window_start_errors"`
	WindowPolled            int64   `json:"window_polled"`
	WindowCompleted         int64   `json:"window_completed"`
	WindowEmptyPolls        int64   `json:"window_empty_polls"`
	WindowPollErrors        int64   `json:"window_poll_errors"`
	WindowCompletionErrors  int64   `json:"window_completion_errors"`
	WindowStartedRPS        float64 `json:"window_started_rps"`
	WindowCompletedRPS      float64 `json:"window_completed_rps"`
	WindowLatencySamples    int     `json:"window_latency_samples"`
	WindowLatencyP50Millis  float64 `json:"window_latency_p50_ms,omitempty"`
	WindowLatencyP95Millis  float64 `json:"window_latency_p95_ms,omitempty"`
	WindowLatencyP99Millis  float64 `json:"window_latency_p99_ms,omitempty"`
	TrackedIncomplete       int     `json:"tracked_incomplete"`
	PrunedTrackedIncomplete int     `json:"pruned_tracked_incomplete"`
}

type cumulativeCounters struct {
	started     int64
	startErr    int64
	polled      int64
	completed   int64
	emptyPolls  int64
	pollErr     int64
	completeErr int64
}

type weightedTaskList struct {
	cfg    taskListConfig
	weight int
}

type workload struct {
	taskLists []taskListConfig
	events    []traceEvent
	duration  time.Duration
}

type labClients struct {
	dispatcher       *yarpc.Dispatcher
	membershipDaemon membership.Resolver
}

type staticPeerProvider struct {
	members []membership.HostInfo
}

func newStats() *stats {
	return &stats{
		workflowStarts: make(map[string]time.Time),
	}
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to the matching lab scenario yaml")
	flag.Parse()

	if configPath == "" {
		fmt.Fprintln(os.Stderr, "--config is required")
		os.Exit(2)
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(1)
	}
	if cfg.RunID == "" {
		cfg.RunID = time.Now().UTC().Format("20060102T150405.000000000")
	}

	workload, err := buildWorkload(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build workload: %v\n", err)
		os.Exit(1)
	}
	if cfg.Duration <= 0 {
		cfg.Duration = workload.duration
	}

	clients, frontend, _, err := newClients(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create clients: %v\n", err)
		os.Exit(1)
	}
	defer clients.Close()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	domainID, err := ensureDomain(ctx, frontend, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ensure domain: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf(
		"matching lab started: domain=%s frontend=%s duration=%s\n",
		cfg.Domain,
		cfg.FrontendGRPCEndpoint,
		cfg.Duration,
	)
	fmt.Printf("resolved domain id: %s\n", domainID)
	fmt.Printf("run id: %s\n", cfg.RunID)
	fmt.Printf("workload prepared: tasklists=%d trace_events=%d\n", len(workload.taskLists), len(workload.events))

	st := newStats()
	var wg sync.WaitGroup

	for _, taskList := range workload.taskLists {
		for poller := 0; poller < taskList.Pollers; poller++ {
			wg.Add(1)
			go runPoller(ctx, &wg, frontend, cfg.Domain, taskList, poller, cfg.PollTimeout, st)
		}
	}

	wg.Add(1)
	if cfg.Trace.enabled() {
		go runTraceGenerator(ctx, &wg, frontend, cfg.Domain, workload.events, st)
	} else {
		go runGenerator(ctx, &wg, frontend, cfg.Domain, cfg.RunID, workload.taskLists, cfg.Generator.RatePerSecond, st)
	}

	wg.Add(1)
	go runSummary(ctx, &wg, cfg.SummaryInterval, st)

	wg.Wait()

	fmt.Printf(
		"matching lab finished: started=%d start_errors=%d polled=%d completed=%d empty_polls=%d poll_errors=%d completion_errors=%d\n",
		st.started.Load(),
		st.startErr.Load(),
		st.polled.Load(),
		st.completed.Load(),
		st.emptyPolls.Load(),
		st.pollErr.Load(),
		st.completeErr.Load(),
	)
}

func loadConfig(path string) (*config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *config) validate() error {
	if c.FrontendGRPCEndpoint == "" {
		return errors.New("frontend_grpc_endpoint is required")
	}
	if c.MatchingGRPCEndpoint == "" && len(c.MatchingPeers) == 0 {
		return errors.New("matching_grpc_endpoint or matching_peers is required")
	}
	if c.Domain == "" {
		return errors.New("domain is required")
	}
	if c.RetentionDays <= 0 {
		c.RetentionDays = 1
	}
	if c.PollTimeout <= 0 {
		c.PollTimeout = 15 * time.Second
	}
	if c.SummaryInterval <= 0 {
		c.SummaryInterval = 10 * time.Second
	}
	c.Trace.setDefaults()
	if c.Trace.enabled() {
		return c.validateTrace()
	}
	if c.Duration <= 0 {
		return errors.New("duration must be greater than zero")
	}
	if c.Generator.RatePerSecond <= 0 {
		return errors.New("generator.rate_per_second must be greater than zero")
	}
	if len(c.TaskLists) == 0 {
		return errors.New("at least one task list is required")
	}

	for i := range c.TaskLists {
		taskList := &c.TaskLists[i]
		if taskList.Name == "" {
			return fmt.Errorf("task_lists[%d].name is required", i)
		}
		if taskList.Weight <= 0 {
			return fmt.Errorf("task_lists[%d].weight must be greater than zero", i)
		}
		if taskList.Pollers <= 0 {
			return fmt.Errorf("task_lists[%d].pollers must be greater than zero", i)
		}
		if taskList.ProcessTime < 0 {
			return fmt.Errorf("task_lists[%d].process_time must not be negative", i)
		}
	}

	return nil
}

func (c *config) validateTrace() error {
	if c.Trace.Path == "" {
		return errors.New("trace.path is required")
	}
	if c.Trace.Interval <= 0 {
		return errors.New("trace.interval must be greater than zero")
	}
	if c.Trace.QPSScale <= 0 {
		return errors.New("trace.qps_scale must be greater than zero")
	}
	if c.Trace.TimeScale <= 0 {
		return errors.New("trace.time_scale must be greater than zero")
	}
	if c.Trace.TopN < 0 {
		return errors.New("trace.top_n must not be negative")
	}
	if c.Trace.Rows < 0 {
		return errors.New("trace.rows must not be negative")
	}
	if c.Trace.StartRow < 0 {
		return errors.New("trace.start_row must not be negative")
	}
	if c.Trace.PollerCapacityQPS <= 0 {
		return errors.New("trace.poller_capacity_qps must be greater than zero")
	}
	if c.Trace.ProcessTime < 0 {
		return errors.New("trace.process_time must not be negative")
	}
	return nil
}

func buildWorkload(cfg *config) (*workload, error) {
	if cfg.Trace.enabled() {
		return buildTraceWorkload(cfg.Trace, cfg.RunID)
	}
	return &workload{
		taskLists: cfg.TaskLists,
		duration:  cfg.Duration,
	}, nil
}

func newClients(cfg *config) (*labClients, frontendClient.Client, matchingClient.Client, error) {
	logger := log.NewNoop()
	metricsClient := metrics.NewNoopMetricsClient()

	grpcTransport := grpc.NewTransport(
		grpc.ClientMaxRecvMsgSize(32*1024*1024),
		grpc.ServerMaxRecvMsgSize(32*1024*1024),
	)
	tchannelTransport, err := tchannel.NewTransport(tchannel.ServiceName("cadence-matching-lab"))
	if err != nil {
		return nil, nil, nil, err
	}

	outboundsBuilder := rpc.CombineOutbounds(
		rpc.NewSingleGRPCOutboundBuilder(service.Frontend, service.Frontend, cfg.FrontendGRPCEndpoint),
		buildMatchingOutbound(cfg, logger, metricsClient),
	)
	outbounds, err := outboundsBuilder.Build(grpcTransport, tchannelTransport)
	if err != nil {
		return nil, nil, nil, err
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      "cadence-matching-lab",
		Outbounds: outbounds.Outbounds,
	})
	if err := dispatcher.Start(); err != nil {
		return nil, nil, nil, err
	}

	frontendConfig := dispatcher.ClientConfig(service.Frontend)
	matchingConfig := dispatcher.ClientConfig(service.Matching)

	frontend := grpcClient.NewFrontendClient(
		apiv1.NewDomainAPIYARPCClient(frontendConfig),
		apiv1.NewWorkflowAPIYARPCClient(frontendConfig),
		apiv1.NewWorkerAPIYARPCClient(frontendConfig),
		apiv1.NewVisibilityAPIYARPCClient(frontendConfig),
		apiv1.NewScheduleAPIYARPCClient(frontendConfig),
	)
	rawMatching := grpcClient.NewMatchingClient(matchingv1.NewMatchingAPIYARPCClient(matchingConfig))

	if len(cfg.MatchingPeers) == 0 {
		return &labClients{dispatcher: dispatcher}, frontend, rawMatching, nil
	}

	members, err := resolveMatchingPeers(cfg.MatchingPeers)
	if err != nil {
		dispatcher.Stop()
		return nil, nil, nil, err
	}
	outbounds.UpdatePeers(service.Matching, members)

	resolver, err := newStaticResolver(members, logger, metricsClient)
	if err != nil {
		dispatcher.Stop()
		return nil, nil, nil, err
	}
	resolver.Start()

	domainIDToName := func(string) (string, error) {
		return cfg.Domain, nil
	}
	dc := dynamicconfig.NewNopCollection()
	partitionConfigProvider := matchingClient.NewPartitionConfigProvider(logger, metricsClient, domainIDToName, dc)
	defaultLoadBalancer := matchingClient.NewLoadBalancer(partitionConfigProvider)
	roundRobinLoadBalancer := matchingClient.NewRoundRobinLoadBalancer(partitionConfigProvider)
	weightedLoadBalancer := matchingClient.NewWeightedLoadBalancer(roundRobinLoadBalancer, partitionConfigProvider, logger)
	isolationLoadBalancer := matchingClient.NewIsolationLoadBalancer(weightedLoadBalancer, partitionConfigProvider, domainIDToName, dc)
	loadBalancers := map[string]matchingClient.LoadBalancer{
		"random":      defaultLoadBalancer,
		"round-robin": roundRobinLoadBalancer,
		"weighted":    weightedLoadBalancer,
		"isolation":   isolationLoadBalancer,
	}
	matching := matchingClient.NewClient(
		rawMatching,
		matchingClient.NewPeerResolver(resolver, membership.PortGRPC),
		matchingClient.NewMultiLoadBalancer(defaultLoadBalancer, loadBalancers, domainIDToName, dc, logger),
		partitionConfigProvider,
	)

	return &labClients{
		dispatcher:       dispatcher,
		membershipDaemon: resolver,
	}, frontend, matching, nil
}

func buildMatchingOutbound(cfg *config, logger log.Logger, metricsClient metrics.Client) rpc.OutboundsBuilder {
	if len(cfg.MatchingPeers) == 0 {
		return rpc.NewSingleGRPCOutboundBuilder(service.Matching, service.Matching, cfg.MatchingGRPCEndpoint)
	}

	return rpc.NewDirectOutboundBuilder(
		service.Matching,
		true,
		nil,
		rpc.NewDirectPeerChooserFactory(service.Matching, logger, metricsClient),
		nil,
	)
}

func newStaticResolver(
	members []membership.HostInfo,
	logger log.Logger,
	metricsClient metrics.Client,
) (membership.Resolver, error) {
	provider := &staticPeerProvider{members: members}
	ring := membership.NewHashring(
		service.Matching,
		provider,
		clock.NewRealTimeSource(),
		logger,
		metricsClient.Scope(metrics.HashringScope),
	)
	return membership.NewResolver(provider, metricsClient, logger, map[string]membership.SingleProvider{
		service.Matching: ring,
	})
}

func resolveMatchingPeers(targets []string) ([]membership.HostInfo, error) {
	const (
		defaultGRPCPort     = 7835
		defaultTChannelPort = 7935
	)

	members := make([]membership.HostInfo, 0, len(targets))
	for _, target := range targets {
		host, grpcPort, err := parsePeerTarget(target, defaultGRPCPort)
		if err != nil {
			return nil, err
		}

		ip, err := resolvePeerHost(host)
		if err != nil {
			return nil, fmt.Errorf("resolve matching peer %q: %w", target, err)
		}

		identity := net.JoinHostPort(ip, strconv.Itoa(defaultTChannelPort))
		members = append(members, membership.NewDetailedHostInfo(identity, identity, membership.PortMap{
			membership.PortGRPC:     uint16(grpcPort),
			membership.PortTchannel: uint16(defaultTChannelPort),
		}))
	}

	return members, nil
}

func parsePeerTarget(target string, defaultPort int) (string, int, error) {
	if _, _, err := net.SplitHostPort(target); err == nil {
		host, port, err := net.SplitHostPort(target)
		if err != nil {
			return "", 0, err
		}
		grpcPort, err := strconv.Atoi(port)
		if err != nil {
			return "", 0, err
		}
		return host, grpcPort, nil
	}

	return target, defaultPort, nil
}

func resolvePeerHost(host string) (string, error) {
	if ip := net.ParseIP(host); ip != nil {
		return ip.String(), nil
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return "", err
	}
	for _, ip := range ips {
		if ipv4 := ip.To4(); ipv4 != nil {
			return ipv4.String(), nil
		}
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("no addresses returned")
	}
	return ips[0].String(), nil
}

func (c *labClients) Close() {
	if c.membershipDaemon != nil {
		c.membershipDaemon.Stop()
	}
	if c.dispatcher != nil {
		c.dispatcher.Stop()
	}
}

func (p *staticPeerProvider) Start() {}

func (p *staticPeerProvider) Stop() {}

func (p *staticPeerProvider) GetMembers(string) ([]membership.HostInfo, error) {
	return append([]membership.HostInfo(nil), p.members...), nil
}

func (p *staticPeerProvider) WhoAmI() (membership.HostInfo, error) {
	if len(p.members) == 0 {
		return membership.HostInfo{}, errors.New("no matching peers configured")
	}
	return p.members[0], nil
}

func (p *staticPeerProvider) SelfEvict() error {
	return nil
}

func (p *staticPeerProvider) Subscribe(string, func(membership.ChangedEvent)) error {
	return nil
}

func ensureDomain(ctx context.Context, frontend frontendClient.Client, cfg *config) (string, error) {
	deadline := time.Now().Add(2 * time.Minute)
	var lastErr error

	for {
		registerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := frontend.RegisterDomain(registerCtx, &types.RegisterDomainRequest{
			Name:                                   cfg.Domain,
			WorkflowExecutionRetentionPeriodInDays: cfg.RetentionDays,
		})
		cancel()
		if err != nil {
			var alreadyExists *types.DomainAlreadyExistsError
			if !errors.As(err, &alreadyExists) {
				lastErr = err
				if ctx.Err() != nil || time.Now().After(deadline) {
					return "", lastErr
				}
				time.Sleep(time.Second)
				continue
			}
		}

		describeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		domain, err := frontend.DescribeDomain(describeCtx, &types.DescribeDomainRequest{Name: &cfg.Domain})
		cancel()
		if err == nil {
			return domain.GetDomainInfo().GetUUID(), nil
		}

		lastErr = err
		if ctx.Err() != nil || time.Now().After(deadline) {
			return "", lastErr
		}
		time.Sleep(time.Second)
	}
}

func runGenerator(
	ctx context.Context,
	wg *sync.WaitGroup,
	frontend frontendClient.Client,
	domainName string,
	runID string,
	taskLists []taskListConfig,
	ratePerSecond int,
	st *stats,
) {
	defer wg.Done()

	chooser := makeWeightedTaskLists(taskLists)
	ticker := time.NewTicker(time.Second / time.Duration(ratePerSecond))
	defer ticker.Stop()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var scheduleID int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			taskList := chooseTaskList(rng, chooser)
			currentScheduleID := atomic.AddInt64(&scheduleID, 1)

			reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			workflowID := fmt.Sprintf("kind-lab-%s-workflow-%d", runID, currentScheduleID)
			startedAt := time.Now()
			err := startWorkflow(reqCtx, frontend, domainName, taskList.cfg.Name, workflowID, uuid.New())
			cancel()
			if err != nil {
				st.startErr.Add(1)
				fmt.Printf("generator error: tasklist=%s err=%v\n", taskList.cfg.Name, err)
				continue
			}

			st.started.Add(1)
			st.recordWorkflowStart(workflowID, startedAt)
		}
	}
}

func runTraceGenerator(
	ctx context.Context,
	wg *sync.WaitGroup,
	frontend frontendClient.Client,
	domainName string,
	events []traceEvent,
	st *stats,
) {
	defer wg.Done()

	start := time.Now()
	for _, event := range events {
		timer := time.NewTimer(time.Until(start.Add(event.at)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		startedAt := time.Now()
		err := startWorkflow(reqCtx, frontend, domainName, event.taskList, event.workflowID, uuid.New())
		cancel()
		if err != nil {
			st.startErr.Add(1)
			fmt.Printf("trace generator error: at=%s tasklist=%s workflow=%s err=%v\n", event.at, event.taskList, event.workflowID, err)
			continue
		}
		st.started.Add(1)
		st.recordWorkflowStart(event.workflowID, startedAt)
	}
}

func startWorkflow(
	ctx context.Context,
	frontend frontendClient.Client,
	domainName string,
	taskList string,
	workflowID string,
	requestID string,
) error {
	_, err := frontend.StartWorkflowExecution(ctx, &types.StartWorkflowExecutionRequest{
		Domain:     domainName,
		WorkflowID: workflowID,
		WorkflowType: &types.WorkflowType{
			Name: "kind-lab-benchmark",
		},
		TaskList: &types.TaskList{
			Name: taskList,
			Kind: types.TaskListKindNormal.Ptr(),
		},
		ExecutionStartToCloseTimeoutSeconds: int32Ptr(300),
		TaskStartToCloseTimeoutSeconds:      int32Ptr(30),
		Identity:                            "kind-lab-generator",
		RequestID:                           requestID,
		WorkflowIDReusePolicy:               types.WorkflowIDReusePolicyAllowDuplicate.Ptr(),
	})
	return err
}

func runPoller(
	ctx context.Context,
	wg *sync.WaitGroup,
	frontend frontendClient.Client,
	domainName string,
	taskList taskListConfig,
	pollerNumber int,
	pollTimeout time.Duration,
	st *stats,
) {
	defer wg.Done()

	identity := fmt.Sprintf("kind-lab-poller-%s-%d", taskList.Name, pollerNumber)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reqCtx, cancel := context.WithTimeout(ctx, pollTimeout)
		resp, err := frontend.PollForDecisionTask(reqCtx, &types.PollForDecisionTaskRequest{
			Domain: domainName,
			TaskList: &types.TaskList{
				Name: taskList.Name,
				Kind: types.TaskListKindNormal.Ptr(),
			},
			Identity: identity,
		})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if yarpcerrors.FromError(err).Code() == yarpcerrors.CodeDeadlineExceeded {
				st.emptyPolls.Add(1)
				continue
			}

			st.pollErr.Add(1)
			fmt.Printf("poller error: tasklist=%s identity=%s err=%v\n", taskList.Name, identity, err)
			continue
		}

		if resp == nil || len(resp.TaskToken) == 0 {
			st.emptyPolls.Add(1)
			continue
		}

		st.polled.Add(1)
		if taskList.ProcessTime > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(taskList.ProcessTime):
			}
		}

		reqCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		_, err = frontend.RespondDecisionTaskCompleted(reqCtx, &types.RespondDecisionTaskCompletedRequest{
			TaskToken: resp.TaskToken,
			Identity:  identity,
			Decisions: []*types.Decision{
				{
					DecisionType: types.DecisionTypeCompleteWorkflowExecution.Ptr(),
					CompleteWorkflowExecutionDecisionAttributes: &types.CompleteWorkflowExecutionDecisionAttributes{
						Result: []byte("ok"),
					},
				},
			},
		})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			st.completeErr.Add(1)
			if resp.WorkflowExecution != nil {
				st.removeWorkflowStart(resp.WorkflowExecution.GetWorkflowID())
			}
			fmt.Printf("completion error: tasklist=%s identity=%s err=%v\n", taskList.Name, identity, err)
			continue
		}

		st.completed.Add(1)
		if resp.WorkflowExecution != nil {
			st.recordWorkflowCompletion(resp.WorkflowExecution.GetWorkflowID(), time.Now())
		}
	}
}

func (s *stats) recordWorkflowStart(workflowID string, startedAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.workflowStarts[workflowID] = startedAt
}

func (s *stats) recordWorkflowCompletion(workflowID string, completedAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	startedAt, ok := s.workflowStarts[workflowID]
	if !ok {
		return
	}
	delete(s.workflowStarts, workflowID)

	latency := completedAt.Sub(startedAt)
	if latency >= 0 {
		s.latencies = append(s.latencies, latency)
	}
}

func (s *stats) removeWorkflowStart(workflowID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.workflowStarts, workflowID)
}

func (s *stats) snapshot(now, start time.Time, window time.Duration, previous cumulativeCounters) (summarySnapshot, cumulativeCounters) {
	current := cumulativeCounters{
		started:     s.started.Load(),
		startErr:    s.startErr.Load(),
		polled:      s.polled.Load(),
		completed:   s.completed.Load(),
		emptyPolls:  s.emptyPolls.Load(),
		pollErr:     s.pollErr.Load(),
		completeErr: s.completeErr.Load(),
	}

	s.mu.Lock()
	latencies := append([]time.Duration(nil), s.latencies...)
	s.latencies = s.latencies[:0]
	pruned := s.pruneTrackedWorkflowsLocked(now, 10*time.Minute)
	trackedIncomplete := len(s.workflowStarts)
	s.mu.Unlock()

	windowSeconds := window.Seconds()
	snapshot := summarySnapshot{
		AtSeconds:               now.Sub(start).Seconds(),
		WindowSeconds:           windowSeconds,
		Started:                 current.started,
		StartErrors:             current.startErr,
		Polled:                  current.polled,
		Completed:               current.completed,
		EmptyPolls:              current.emptyPolls,
		PollErrors:              current.pollErr,
		CompletionErrors:        current.completeErr,
		WindowStarted:           current.started - previous.started,
		WindowStartErrors:       current.startErr - previous.startErr,
		WindowPolled:            current.polled - previous.polled,
		WindowCompleted:         current.completed - previous.completed,
		WindowEmptyPolls:        current.emptyPolls - previous.emptyPolls,
		WindowPollErrors:        current.pollErr - previous.pollErr,
		WindowCompletionErrors:  current.completeErr - previous.completeErr,
		WindowLatencySamples:    len(latencies),
		TrackedIncomplete:       trackedIncomplete,
		PrunedTrackedIncomplete: pruned,
	}
	if windowSeconds > 0 {
		snapshot.WindowStartedRPS = float64(snapshot.WindowStarted) / windowSeconds
		snapshot.WindowCompletedRPS = float64(snapshot.WindowCompleted) / windowSeconds
	}
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool {
			return latencies[i] < latencies[j]
		})
		snapshot.WindowLatencyP50Millis = percentileDurationMillis(latencies, 0.50)
		snapshot.WindowLatencyP95Millis = percentileDurationMillis(latencies, 0.95)
		snapshot.WindowLatencyP99Millis = percentileDurationMillis(latencies, 0.99)
	}

	return snapshot, current
}

func (s *stats) pruneTrackedWorkflowsLocked(now time.Time, maxAge time.Duration) int {
	pruned := 0
	for workflowID, startedAt := range s.workflowStarts {
		if now.Sub(startedAt) > maxAge {
			delete(s.workflowStarts, workflowID)
			pruned++
		}
	}
	return pruned
}

func percentileDurationMillis(sorted []time.Duration, percentile float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if percentile <= 0 {
		return durationMillis(sorted[0])
	}
	if percentile >= 1 {
		return durationMillis(sorted[len(sorted)-1])
	}

	idx := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return durationMillis(sorted[idx])
}

func durationMillis(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

func runSummary(ctx context.Context, wg *sync.WaitGroup, every time.Duration, st *stats) {
	defer wg.Done()

	ticker := time.NewTicker(every)
	defer ticker.Stop()

	start := time.Now()
	previous := cumulativeCounters{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			snapshot, current := st.snapshot(now, start, every, previous)
			previous = current

			fmt.Printf(
				"summary: t=%.0fs started=%d start_errors=%d polled=%d completed=%d empty_polls=%d poll_errors=%d completion_errors=%d completed_rps=%.3f p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f latency_samples=%d incomplete=%d\n",
				snapshot.AtSeconds,
				snapshot.Started,
				snapshot.StartErrors,
				snapshot.Polled,
				snapshot.Completed,
				snapshot.EmptyPolls,
				snapshot.PollErrors,
				snapshot.CompletionErrors,
				snapshot.WindowCompletedRPS,
				snapshot.WindowLatencyP50Millis,
				snapshot.WindowLatencyP95Millis,
				snapshot.WindowLatencyP99Millis,
				snapshot.WindowLatencySamples,
				snapshot.TrackedIncomplete,
			)
			writeSummaryJSON(snapshot)
		}
	}
}

func writeSummaryJSON(snapshot summarySnapshot) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		fmt.Printf("summary_json_error: %v\n", err)
		return
	}
	fmt.Printf("summary_json: %s\n", data)
}

func int32Ptr(v int32) *int32 {
	return &v
}

func makeWeightedTaskLists(taskLists []taskListConfig) []weightedTaskList {
	weighted := make([]weightedTaskList, 0, len(taskLists))
	for _, taskList := range taskLists {
		weighted = append(weighted, weightedTaskList{cfg: taskList, weight: taskList.Weight})
	}

	return weighted
}

func chooseTaskList(rng *rand.Rand, taskLists []weightedTaskList) weightedTaskList {
	totalWeight := 0
	for _, taskList := range taskLists {
		totalWeight += taskList.weight
	}

	roll := rng.Intn(totalWeight)
	for _, taskList := range taskLists {
		if roll < taskList.weight {
			return taskList
		}
		roll -= taskList.weight
	}

	return taskLists[len(taskLists)-1]
}
