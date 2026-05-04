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

- `cadence-matching-a-0`: `1` CPU
- `cadence-matching-b-0`: `2` CPU
- `cadence-matching-c-0`: `3` CPU

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

The default smoke-test scenario is:

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

The deterministic trace replay scenario is:

- `environment/kind-lab/scenarios/trace-21-12.yaml`

It replays `21-12_00-06_fixed.csv` as fixed per-task-list start times. Each CSV column becomes a task list, each CSV row is a load window, and the number of workflow starts in that window is:

```text
round(csv_qps * qps_scale * interval_seconds)
```

Events are spread evenly across each window and sorted by time, task list, and workflow ID. This gives each algorithm branch the same input schedule. Put the trace CSV at `environment/kind-lab/traces/21-12_00-06_fixed.csv` before building the Docker image. The default trace scenario uses `qps_scale: 0.25`, `top_n: 1000`, and the first 180 rows.

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

Reset the lab namespace before a clean experiment run:

```bash
./environment/kind-lab/scripts/reset.sh
```

Deploy heterogeneous matching resources:

```bash
./environment/kind-lab/scripts/deploy.sh heterogeneous
```

Deploy Prometheus and Grafana inside the kind cluster:

```bash
./environment/kind-lab/scripts/deploy-observability.sh
```

Open Grafana through a local port-forward:

```bash
kubectl -n cadence-kind-lab port-forward svc/grafana 3000:3000
```

Then open `http://localhost:3000`. Grafana is configured with a default
Prometheus datasource named `Prometheus`, so implementation-specific dashboards
can be imported directly.

## Run The Workload

```bash
./environment/kind-lab/scripts/run-load.sh
```

Run the deterministic trace replay:

```bash
./environment/kind-lab/scripts/run-load.sh trace-21-12
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

Save the workload log when running experiments so it can be plotted later:

```bash
./environment/kind-lab/scripts/run-load.sh trace-21-12 | tee clean.log
```

`cadence-matching-lab` emits `summary_json` lines with per-window throughput
and latency data. Plot completed throughput and p95 latency for multiple runs:

```bash
./environment/kind-lab/scripts/plot-matching-lab.py \
  --run clean=clean.log \
  --run latency=latency.log \
  --run cpuseconds=cpuseconds.log \
  --output-dir environment/kind-lab/results/plots
```

This writes:

- `matching-lab-throughput.png`
- `matching-lab-p95-latency.png`

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
