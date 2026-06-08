#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE="${NAMESPACE:-cadence-kind-lab}"

RUN_NAME=""
SCENARIO="trace-21-12"
MATCHING_HETEROGENEITY_PROFILE="${MATCHING_HETEROGENEITY_PROFILE:-equal_cores}"
GREEDY_HETEROGENEITY_MODE="${GREEDY_HETEROGENEITY_MODE:-latency}"
GREEDY_MOVE_SCORING_MODE="${GREEDY_MOVE_SCORING_MODE:-benefit}"
GREEDY_MOVE_PENALTY_COEFFICIENT="${GREEDY_MOVE_PENALTY_COEFFICIENT:-0.2}"
GREEDY_CPU_SECONDS_SMOOTHING_TAU="${GREEDY_CPU_SECONDS_SMOOTHING_TAU:-3m}"
GREEDY_ENABLE_SWAP="${GREEDY_ENABLE_SWAP:-false}"
GREEDY_ENABLE_MULTI_MOVE="${GREEDY_ENABLE_MULTI_MOVE:-false}"
MATCHING_ENABLE_ADAPTIVE_SCALER="${MATCHING_ENABLE_ADAPTIVE_SCALER:-false}"
MATCHING_NUM_TASKLIST_READ_PARTITIONS="${MATCHING_NUM_TASKLIST_READ_PARTITIONS:-1}"
MATCHING_NUM_TASKLIST_WRITE_PARTITIONS="${MATCHING_NUM_TASKLIST_WRITE_PARTITIONS:-1}"
MATCHING_EXECUTOR_COUNT="${MATCHING_EXECUTOR_COUNT:-1}"
SAMPLE_INTERVAL_SECONDS="30"
DURATION_SECONDS="3600"
SETTLE_SECONDS="120"
READINESS_TIMEOUT_SECONDS="300"
BUILD_IMAGE="false"
BUILD_IMAGE_NO_CACHE="false"
CREATE_CLUSTER="false"
PORT_FORWARD="false"
GRAFANA_REMOTE_PORT="${GRAFANA_REMOTE_PORT:-3000}"

RUN_PIDS=()

usage() {
  cat <<'EOF'
Usage:
  run-controlled-kind-lab.sh --run RUN_NAME [options]

Prepares a clean kind-lab run, deploys the cluster, then runs:
  - sample-utilization.sh in the background
  - run-load.sh in the foreground (matching-lab workload)
  - optional CPU debug log collection (cpu_seconds mode only)

Options:
  --run NAME                    Result stem, e.g. latency-1hr-2500-final
  --scenario NAME               Scenario passed to run-load.sh (default: trace-21-12)
  --profile NAME                MATCHING_HETEROGENEITY_PROFILE (default: equal_cores)
  --heterogeneity-mode MODE     GREEDY_HETEROGENEITY_MODE: off|latency|cpu_seconds (default: latency)
  --move-scoring-mode MODE      GREEDY_MOVE_SCORING_MODE: benefit|cost_aware (default: benefit)
  --penalty VALUE               GREEDY_MOVE_PENALTY_COEFFICIENT (default: 0.2)
  --cpu-tau DURATION            GREEDY_CPU_SECONDS_SMOOTHING_TAU (default: 3m)
  --enable-swap BOOL            GREEDY_ENABLE_SWAP: true|false (default: false)
  --enable-multi-move BOOL      GREEDY_ENABLE_MULTI_MOVE: true|false (default: false)
  --adaptive-scaler BOOL        MATCHING_ENABLE_ADAPTIVE_SCALER: true|false (default: false)
  --read-partitions N           MATCHING_NUM_TASKLIST_READ_PARTITIONS (default: 1)
  --write-partitions N          MATCHING_NUM_TASKLIST_WRITE_PARTITIONS (default: 1)
  --executors N                 MATCHING_EXECUTOR_COUNT: 1|2|3 (default: 1)
  --sample-interval SECONDS     Utilization sample interval (default: 30)
  --duration SECONDS            Run/sampling duration (default: 3600)
  --settle-seconds SECONDS      Wait after deploy before starting run (default: 120)
  --readiness-timeout SECONDS   Wait for shard-distributor assignment readiness (default: 300)
  --build-image                 Build cadence-kind-lab image first
  --no-cache-build              Pass --no-cache to docker build (with --build-image)
  --create-cluster              Create/load kind cluster first
  --port-forward                Start Grafana/Prometheus port-forwards in the background
  -h, --help                    Show this help

Stop a background run:
  kill $(cat environment/kind-lab/results/RUN_NAME/run.pids)

Environment variables with the same names can also be used for the greedy/profile settings.
EOF
}

stop_previous_run() {
  local pids_file="$1"
  if [[ ! -f "$pids_file" ]]; then
    return 0
  fi

  echo "stopping previous background tasks from $pids_file"
  while IFS= read -r pid; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done <"$pids_file"
  rm -f "$pids_file"
}

track_pid() {
  local pid="$1"
  local pids_file="$2"
  RUN_PIDS+=("$pid")
  echo "$pid" >>"$pids_file"
}

wait_for_background_tasks() {
  local failed=0
  for pid in "${RUN_PIDS[@]}"; do
    if ! wait "$pid"; then
      failed=1
    fi
  done
  return "$failed"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run)
      RUN_NAME="$2"
      shift 2
      ;;
    --scenario)
      SCENARIO="$2"
      shift 2
      ;;
    --profile)
      MATCHING_HETEROGENEITY_PROFILE="$2"
      shift 2
      ;;
    --heterogeneity-mode)
      GREEDY_HETEROGENEITY_MODE="$2"
      shift 2
      ;;
    --move-scoring-mode)
      GREEDY_MOVE_SCORING_MODE="$2"
      shift 2
      ;;
    --penalty)
      GREEDY_MOVE_PENALTY_COEFFICIENT="$2"
      shift 2
      ;;
    --cpu-tau)
      GREEDY_CPU_SECONDS_SMOOTHING_TAU="$2"
      shift 2
      ;;
    --adaptive-scaler)
      MATCHING_ENABLE_ADAPTIVE_SCALER="$2"
      shift 2
      ;;
    --read-partitions)
      MATCHING_NUM_TASKLIST_READ_PARTITIONS="$2"
      shift 2
      ;;
    --write-partitions)
      MATCHING_NUM_TASKLIST_WRITE_PARTITIONS="$2"
      shift 2
      ;;
    --executors)
      MATCHING_EXECUTOR_COUNT="$2"
      shift 2
      ;;
    --sample-interval)
      SAMPLE_INTERVAL_SECONDS="$2"
      shift 2
      ;;
    --duration)
      DURATION_SECONDS="$2"
      shift 2
      ;;
    --settle-seconds)
      SETTLE_SECONDS="$2"
      shift 2
      ;;
    --readiness-timeout)
      READINESS_TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    --build-image)
      BUILD_IMAGE="true"
      shift
      ;;
    --no-cache-build)
      BUILD_IMAGE_NO_CACHE="true"
      shift
      ;;
    --create-cluster)
      CREATE_CLUSTER="true"
      shift
      ;;
    --port-forward)
      PORT_FORWARD="true"
      shift
      ;;
    --no-attach)
      echo "warning: --no-attach is deprecated (tmux was removed); ignoring" >&2
      shift
      ;;
    --session)
      echo "warning: --session is deprecated (tmux was removed); ignoring" >&2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$RUN_NAME" ]]; then
  echo "--run is required" >&2
  usage >&2
  exit 2
fi

case "$GREEDY_HETEROGENEITY_MODE" in
  off|latency|cpu_seconds) ;;
  *) echo "--heterogeneity-mode must be one of: off, latency, cpu_seconds" >&2; exit 2 ;;
esac
case "$GREEDY_MOVE_SCORING_MODE" in
  benefit|cost_aware) ;;
  *) echo "--move-scoring-mode must be one of: benefit, cost_aware" >&2; exit 2 ;;
esac
case "$MATCHING_HETEROGENEITY_PROFILE" in
  equal_burn|equal_cores|mixed) ;;
  *) echo "--profile must be one of: equal_burn, equal_cores, mixed" >&2; exit 2 ;;
esac
case "$GREEDY_ENABLE_SWAP" in
  true|false) ;;
  *) echo "--enable-swap must be true or false" >&2; exit 2 ;;
esac
case "$GREEDY_ENABLE_MULTI_MOVE" in
  true|false) ;;
  *) echo "--enable-multi-move must be true or false" >&2; exit 2 ;;
esac
case "$MATCHING_ENABLE_ADAPTIVE_SCALER" in
  true|false) ;;
  *) echo "--adaptive-scaler must be true or false" >&2; exit 2 ;;
esac
if ! [[ "$MATCHING_NUM_TASKLIST_READ_PARTITIONS" =~ ^[1-9][0-9]*$ ]]; then
  echo "--read-partitions must be a positive integer" >&2
  exit 2
fi
if ! [[ "$MATCHING_NUM_TASKLIST_WRITE_PARTITIONS" =~ ^[1-9][0-9]*$ ]]; then
  echo "--write-partitions must be a positive integer" >&2
  exit 2
fi
if ! [[ "$MATCHING_EXECUTOR_COUNT" =~ ^[1-3]$ ]]; then
  echo "--executors must be 1, 2, or 3" >&2
  exit 2
fi
if ! [[ "$READINESS_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "--readiness-timeout must be a positive integer" >&2
  exit 2
fi

RESULT_DIR="$ROOT/environment/kind-lab/results"
RUN_DIR="$RESULT_DIR/$RUN_NAME"
CSV_PATH="$RESULT_DIR/$RUN_NAME.csv"
LOG_PATH="$RESULT_DIR/$RUN_NAME.log"
CPU_DEBUG_DIR="$RESULT_DIR/$RUN_NAME-cpu-debug"
SAMPLE_LOG="$RUN_DIR/sample.log"
PIDS_FILE="$RUN_DIR/run.pids"
mkdir -p "$RESULT_DIR" "$RUN_DIR" "$CPU_DEBUG_DIR"
stop_previous_run "$PIDS_FILE"

if [[ "$BUILD_IMAGE" == "true" ]]; then
  if [[ "$BUILD_IMAGE_NO_CACHE" == "true" ]]; then
    CADENCE_KIND_LAB_DOCKER_NO_CACHE=1 "$ROOT/environment/kind-lab/scripts/build-image.sh"
  else
    "$ROOT/environment/kind-lab/scripts/build-image.sh"
  fi
fi
if [[ "$CREATE_CLUSTER" == "true" ]]; then
  "$ROOT/environment/kind-lab/scripts/create-cluster.sh"
fi

"$ROOT/environment/kind-lab/scripts/reset.sh"

RUN_NAME="$RUN_NAME" SCENARIO="$SCENARIO" \
MATCHING_HETEROGENEITY_PROFILE="$MATCHING_HETEROGENEITY_PROFILE" \
GREEDY_HETEROGENEITY_MODE="$GREEDY_HETEROGENEITY_MODE" \
GREEDY_MOVE_SCORING_MODE="$GREEDY_MOVE_SCORING_MODE" \
GREEDY_MOVE_PENALTY_COEFFICIENT="$GREEDY_MOVE_PENALTY_COEFFICIENT" \
GREEDY_CPU_SECONDS_SMOOTHING_TAU="$GREEDY_CPU_SECONDS_SMOOTHING_TAU" \
MATCHING_ENABLE_ADAPTIVE_SCALER="$MATCHING_ENABLE_ADAPTIVE_SCALER" \
MATCHING_NUM_TASKLIST_READ_PARTITIONS="$MATCHING_NUM_TASKLIST_READ_PARTITIONS" \
MATCHING_NUM_TASKLIST_WRITE_PARTITIONS="$MATCHING_NUM_TASKLIST_WRITE_PARTITIONS" \
MATCHING_EXECUTOR_COUNT="$MATCHING_EXECUTOR_COUNT" \
  "$ROOT/environment/kind-lab/scripts/deploy.sh" heterogeneous

"$ROOT/environment/kind-lab/scripts/deploy-observability.sh"

kubectl get configmap kind-lab-run-metadata -n "$NAMESPACE" \
  -o jsonpath='{.data.metadata\.json}' >"$RESULT_DIR/$RUN_NAME-metadata.json" 2>/dev/null || true

echo "settling for ${SETTLE_SECONDS}s before starting workload..."
sleep "$SETTLE_SECONDS"

etcd_alarm="$(kubectl exec -n "$NAMESPACE" etcd-0 -- sh -lc 'ETCDCTL_API=3 /opt/bitnami/etcd/bin/etcdctl --endpoints=http://127.0.0.1:2379 alarm list' 2>/dev/null || true)"
if [[ -n "$etcd_alarm" ]]; then
  echo "etcd alarm is active before workload; refusing to start:" >&2
  echo "$etcd_alarm" >&2
  exit 1
fi

wait_for_shard_assignments() {
  local deadline=$((SECONDS + READINESS_TIMEOUT_SECONDS))
  local assigned_count="0"
  local executor_status_count="0"

  echo "waiting for shard-distributor assignments before starting workload..."
  while true; do
    executor_status_count="$(kubectl exec -n "$NAMESPACE" etcd-0 -- sh -lc '
      ETCDCTL_API=3 /opt/bitnami/etcd/bin/etcdctl \
        --endpoints=http://127.0.0.1:2379 \
        get store/cadence-matching-kind-lab/executors/ --prefix --keys-only \
        | grep /status | wc -l
    ' 2>/dev/null | tr -d '[:space:]' || true)"
    assigned_count="$(kubectl exec -n "$NAMESPACE" etcd-0 -- sh -lc '
      ETCDCTL_API=3 /opt/bitnami/etcd/bin/etcdctl \
        --endpoints=http://127.0.0.1:2379 \
        get store/cadence-matching-kind-lab/executors/ --prefix --keys-only \
        | grep /assigned_state | wc -l
    ' 2>/dev/null | tr -d '[:space:]' || true)"

    executor_status_count="${executor_status_count:-0}"
    assigned_count="${assigned_count:-0}"
    if [[ "$executor_status_count" -ge "$MATCHING_EXECUTOR_COUNT" && "$assigned_count" -ge "$MATCHING_EXECUTOR_COUNT" ]]; then
      echo "shard-distributor assignments ready: executors=$executor_status_count assigned_state=$assigned_count (expected=$MATCHING_EXECUTOR_COUNT)"
      return 0
    fi

    if (( SECONDS >= deadline )); then
      echo "timed out waiting for shard-distributor assignments: executors=$executor_status_count assigned_state=$assigned_count" >&2
      echo "refusing to start workload because runs with missing assigned_state begin with polled=0/completed=0 and are invalid" >&2
      return 1
    fi

    echo "  not ready yet: executors=$executor_status_count assigned_state=$assigned_count"
    sleep 5
  done
}

wait_for_shard_assignments

kubectl delete job matching-lab -n "$NAMESPACE" --ignore-not-found
kubectl wait --for=delete job/matching-lab -n "$NAMESPACE" --timeout=60s >/dev/null 2>&1 || true

cat <<EOF
Starting run: $RUN_NAME
Scenario:              $SCENARIO
Profile:               $MATCHING_HETEROGENEITY_PROFILE
Heterogeneity mode:    $GREEDY_HETEROGENEITY_MODE
Move scoring mode:     $GREEDY_MOVE_SCORING_MODE
Move penalty:          $GREEDY_MOVE_PENALTY_COEFFICIENT
CPU smoothing tau:     $GREEDY_CPU_SECONDS_SMOOTHING_TAU
Adaptive scaler:       $MATCHING_ENABLE_ADAPTIVE_SCALER
Tasklist partitions:   read=$MATCHING_NUM_TASKLIST_READ_PARTITIONS write=$MATCHING_NUM_TASKLIST_WRITE_PARTITIONS
Executor count:        $MATCHING_EXECUTOR_COUNT
Utilization CSV:       $CSV_PATH
Sample log:            $SAMPLE_LOG
Matching log:          $LOG_PATH
CPU debug logs:        $CPU_DEBUG_DIR
Background PIDs file:  $PIDS_FILE

Grafana:  http://localhost:${GRAFANA_REMOTE_PORT}/d/cadence-kind-lab-experiments
Prometheus: http://localhost:9090
EOF

(
  cd "$ROOT"
  ./environment/kind-lab/scripts/sample-utilization.sh \
    "$SAMPLE_INTERVAL_SECONDS" "$DURATION_SECONDS" "$CSV_PATH"
) >"$SAMPLE_LOG" 2>&1 &
track_pid "$!" "$PIDS_FILE"
echo "sample-utilization pid: ${RUN_PIDS[-1]} (log: $SAMPLE_LOG)"

if [[ "$GREEDY_HETEROGENEITY_MODE" == "cpu_seconds" ]]; then
  (
    cd "$ROOT"
    ./environment/kind-lab/scripts/collect-cpu-debug.sh \
      "$CPU_DEBUG_DIR" "$SAMPLE_INTERVAL_SECONDS" "$DURATION_SECONDS"
  ) >"$CPU_DEBUG_DIR/collector.log" 2>&1 &
  track_pid "$!" "$PIDS_FILE"
  echo "cpu debug collector pid: ${RUN_PIDS[-1]} (log: $CPU_DEBUG_DIR/collector.log)"
fi

if [[ "$PORT_FORWARD" == "true" ]]; then
  kubectl -n "$NAMESPACE" port-forward svc/grafana "${GRAFANA_REMOTE_PORT}:3000" \
    >"$RUN_DIR/grafana-port-forward.log" 2>&1 &
  track_pid "$!" "$PIDS_FILE"
  echo "grafana port-forward pid: ${RUN_PIDS[-1]}"

  kubectl -n "$NAMESPACE" port-forward svc/prometheus 9090:9090 \
    >"$RUN_DIR/prometheus-port-forward.log" 2>&1 &
  track_pid "$!" "$PIDS_FILE"
  echo "prometheus port-forward pid: ${RUN_PIDS[-1]}"
else
  echo "port-forward disabled; rerun with --port-forward or run:"
  echo "  kubectl -n $NAMESPACE port-forward svc/grafana ${GRAFANA_REMOTE_PORT}:3000"
  echo "  kubectl -n $NAMESPACE port-forward svc/prometheus 9090:9090"
fi

load_status=0
(
  cd "$ROOT"
  ./environment/kind-lab/scripts/run-load.sh "$SCENARIO"
) 2>&1 | tee "$LOG_PATH" || load_status=$?

echo "matching-lab workload finished"
if ! wait_for_background_tasks; then
  echo "one or more background tasks failed; check $RUN_DIR/*.log" >&2
  exit 1
fi
if [[ "$load_status" -ne 0 ]]; then
  echo "matching-lab workload failed; see $LOG_PATH" >&2
  exit "$load_status"
fi

echo "run complete: $RUN_NAME"
echo "results:"
echo "  utilization: $CSV_PATH"
echo "  workload:    $LOG_PATH"
if [[ "$GREEDY_HETEROGENEITY_MODE" == "cpu_seconds" ]]; then
  echo "  cpu debug:   $CPU_DEBUG_DIR"
fi
