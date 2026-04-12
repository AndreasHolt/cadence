# Kind Lab for P10 Heterogeneous Matching Experiments

This is a local evaluation setup for the P10 master's thesis work on heterogeneous node support and shard load balancing.

It is not intended as an upstream Cadence deployment model or production feature. The goal is to create a repeatable local Kubernetes environment where we can test and compare load-balancing behavior when matching executors have different capacities.

## What This Lab Runs

- A local `kind` Kubernetes cluster.
- Cadence frontend, history, matching, and shard-distributor services.
- Cassandra and etcd inside the cluster.
- Three matching pods with different CPU limits.
- A workload job that creates real Cadence workflow traffic through frontend.

The heterogeneous matching pods are:

- `cadence-matching-a-0`: `500m` CPU
- `cadence-matching-b-0`: `1000m` CPU
- `cadence-matching-c-0`: `2000m` CPU

The setup is used to observe whether shard-distributor load balancing behaves sensibly when executors are not equal.

## Workload Model

The load job is `cadence-matching-lab`.

It does not only call matching directly. It drives a small real workflow loop:

- `StartWorkflowExecution`
- `PollForDecisionTask`
- optional worker-side `process_time`
- `RespondDecisionTaskCompleted`

Each workflow is intentionally simple:

- start workflow
- receive the first decision task
- complete the workflow immediately

This is enough to exercise frontend, history, matching, polling, and completion while keeping the workload easy to control.

## Scenario

The default scenario is:

- `environment/kind-lab/scenarios/hotspot.yaml`

It controls:

- frontend endpoint
- domain
- duration
- request rate
- tasklist weights
- number of pollers per tasklist
- simulated worker processing time per tasklist

The same scenario should be used for homogeneous and heterogeneous runs so the workload stays fixed while only executor capacity changes.

## Important Config

The lab enables shard-distributor ownership for matching:

```yaml
matching.shardDistributionMode: shard_distributor
matching.percentageOnboardedToShardManager: 100
shardDistributor.migrationMode: onboarded
```

The lab also lowers the matching QPS tracker interval:

```yaml
matching.qpsTrackerInterval: 1s
```

This is a lab setting. It makes reported load react quickly enough for short local experiments.

## Local Prerequisites

Install:

- `docker`
- `kubectl`
- `kind`
- `go`
- `make`

The scripts expect Docker Desktop or another working local Docker daemon.

## Files

- Cadence config: `environment/kind-lab/config/cadence-kind-lab.yaml`
- Dynamic config: `environment/kind-lab/config/dynamicconfig-kind-lab.yaml`
- Scenario: `environment/kind-lab/scenarios/hotspot.yaml`
- Bootstrap manifests: `environment/kind-lab/k8s/bootstrap`
- App manifests: `environment/kind-lab/k8s/apps`
- Load job: `environment/kind-lab/k8s/load/matching-lab-job.yaml`
- Helper scripts: `environment/kind-lab/scripts`
- Workload source: `cmd/tools/matchinglab`
- Shard-state formatter: `cmd/tools/shardlabstate`

## Build And Deploy

Build the local Cadence image and load it into kind:

```bash
./environment/kind-lab/scripts/build-image.sh
```

Create the cluster:

```bash
./environment/kind-lab/scripts/create-cluster.sh
```

Deploy heterogeneous matching resources:

```bash
./environment/kind-lab/scripts/deploy.sh heterogeneous
```

## Run The Workload

```bash
./environment/kind-lab/scripts/run-load.sh
```

Expected summary output looks like:

```text
summary: started=... start_errors=... polled=... completed=... empty_polls=... poll_errors=... completion_errors=...
```

The main sanity check is:

- `started > 0`
- `polled > 0`
- `completed > 0`
- errors stay near zero

## Verify State

```bash
./environment/kind-lab/scripts/verify.sh
```

This prints:

- all pods
- matching pod CPU resources
- `gomaxprocs` from each matching pod
- shard-distributor executor state from etcd

The shard-distributor section is the main thing to inspect when testing balancing behavior.

## Homogeneous Vs Heterogeneous

Deploy homogeneous resources:

```bash
./environment/kind-lab/scripts/deploy.sh homogeneous
```

Deploy heterogeneous resources:

```bash
./environment/kind-lab/scripts/deploy.sh heterogeneous
```

Use the same workload scenario for both runs when comparing behavior.

## Evaluation Checklist

For P10, the useful comparison is not just whether the cluster runs. Each experiment should record enough data to compare balancing behavior across different capacity setups.

Recommended baseline flow:

- Deploy homogeneous matching resources.
- Run the same workload scenario.
- Save the workload summary output.
- Save the `verify.sh` shard-distributor state.
- Save matching pod CPU and `gomaxprocs` output.
- Deploy heterogeneous matching resources.
- Run the same workload scenario again.
- Compare shard placement, reported load, throughput, and errors.

Useful signals:

- workflow starts per second
- completed workflows per second
- poll errors and completion errors
- empty polls
- shard load per executor
- matching pod CPU limits
- matching pod `gomaxprocs`
- shard-distributor rebalance decisions from logs

Still useful to add:

- CSV scenario replay for production-like changing load over time.
- Per-tasklist completion counts in `cadence-matching-lab`.
- Basic latency summaries such as p50, p95, and p99 workflow completion latency.
- A script that runs homogeneous and heterogeneous experiments back-to-back and writes results to a timestamped directory.
- A scenario with enough skew to force visible rebalancing decisions.

## Notes

- This lab exists for P10 evaluation, not as an upstream Cadence recommendation.
- The workload is intentionally small and controllable.
- Worker `process_time` happens in the load job, not inside matching.
- This setup creates real service traffic, but it is not a full production benchmark.
- The purpose is to make heterogeneous executor behavior observable enough to evaluate load-balancing changes.
