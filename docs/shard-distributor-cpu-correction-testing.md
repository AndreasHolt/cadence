# Testing Shard Distributor CPU Cost Correction

This guide explains how to set up a local environment to verify the Shard Distributor's CPU cost correction logic. This logic automatically adjusts an executor's capacity weight if its actual CPU usage per load unit deviates from the fleet average.

## Prerequisites

Ensure you have the latest binaries built:
```bash
make bins
```

## 1. Environment Configuration

### Enable SQLite and Debug Logging
Modify `config/development_sqlite.yaml` to enable debug logs, which will show the intermediate CPU correction calculations:

```yaml
# config/development_sqlite.yaml
log:
  level: "debug"
```

### Enable Greedy Load Balancing
CPU corrections only run in `GREEDY` mode. Enable it in the dynamic config:

```yaml
# config/dynamicconfig/development.yaml
shardDistributor.loadBalancingMode:
  - value: "greedy"
```

## 2. Simulated CPU Load

The `sharddistributor-canary` tool has been modified to consume actual CPU time in its background processing loop. The amount of CPU burned is proportional to the reported `shardLoad` and can be further scaled using an environment variable.

*   **Reported Load:** Shard "10" reports `10.0` load units.
*   **Actual CPU:** Controlled by `CANARY_CPU_MULTIPLIER`.

### Note on Multi-threading and GOMAXPROCS
Each shard assigned to an executor runs its own independent background goroutine. Since the canary namespace uses 32 shards by default, the executor process will have many concurrent "threads" trying to burn CPU.

*   The Go runtime automatically parallelizes these goroutines across the CPU cores allowed by `GOMAXPROCS`.
*   An executor with `GOMAXPROCS=1` will cap its total CPU usage at **100% (1.0 cores)**, even if 32 shards are competing for time.
*   An executor with `GOMAXPROCS=2` can use up to **200% (2.0 cores)**.

The CPU correction logic uses this data to calculate the **"Cost per Load Unit"**. If two executors have the same load but one uses more CPU cores to handle it (due to a higher `CANARY_CPU_MULTIPLIER`), the leader will identify the inefficient executor and reduce its capacity weight.

## 3. Step-by-Step Test Scenario

This scenario uses 3 executors with different hardware sizes (`GOMAXPROCS`) and different efficiency profiles (`MULTIPLIER`).

### Step 1: Start the Cadence Server
```bash
./cadence-server --zone sqlite start
```

### Step 2: Start Executor A (Small, Efficient)
Reports 1 core, performs "standard" work.
```bash
GOMAXPROCS=1 CANARY_CPU_MULTIPLIER=1.0 ./sharddistributor-canary start --canary-grpc-port 7953
```

### Step 3: Start Executor B (Small, Inefficient)
Reports 1 core, but performs 50% more work per load unit than A.
```bash
GOMAXPROCS=1 CANARY_CPU_MULTIPLIER=1.5 ./sharddistributor-canary start --canary-grpc-port 7954
```

### Step 4: Start Executor C (Large, Efficient)
Reports 2 cores, performs "standard" work.
```bash
GOMAXPROCS=2 CANARY_CPU_MULTIPLIER=1.0 ./sharddistributor-canary start --canary-grpc-port 7955
```

## 4. Verifying Results

### Monitor Logs
Search the server output for "cpu correction debug". After approximately 60 seconds (the warm-up period for the OLS regression), you will see entries like:

```text
"cpu correction debug" ... "executor_id": "127.0.0.1:7954" ... "cpu_cost_correction": 0.75 ... "capacity_weight": 0.75
```

### Expected Outcome
The leader will calculate the following corrections:

| Executor | Initial Weight | Work Multiplier | Expected Correction | Final Weight |
| :--- | :--- | :--- | :--- | :--- |
| **A** | 1.0 | 1.0 | ~1.12 | **1.12** |
| **B** | 1.0 | 1.5 | **~0.75** | **0.75** |
| **C** | 2.0 | 1.0 | ~1.12 | **2.25** |

The Shard Distributor will then move shards until the distribution matches these final weights. Executor C should end up with ~3x as many shards as Executor B.
