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
GREEDY_CPU_SECONDS_SMOOTHING_TAU="${GREEDY_CPU_SECONDS_SMOOTHING_TAU:-10m}"
SAMPLE_INTERVAL_SECONDS="30"
DURATION_SECONDS="3600"
SETTLE_SECONDS="120"
SESSION_NAME=""
BUILD_IMAGE="false"
CREATE_CLUSTER="false"
ATTACH="true"

usage() {
  cat <<'EOF'
Usage:
  run-controlled-kind-lab.sh --run RUN_NAME [options]

Prepares a clean kind-lab run, then starts a tmux session with:
  pane 1: sample-utilization.sh
  pane 2: run-load.sh | tee results/RUN_NAME.log
  pane 3: kubectl watch pane

Options:
  --run NAME                    Result stem, e.g. latency-1hr-2500-final
  --scenario NAME               Scenario passed to run-load.sh (default: trace-21-12)
  --profile NAME                MATCHING_HETEROGENEITY_PROFILE (default: equal_cores)
  --heterogeneity-mode MODE     GREEDY_HETEROGENEITY_MODE: off|latency|cpu_seconds (default: latency)
  --move-scoring-mode MODE      GREEDY_MOVE_SCORING_MODE: benefit|cost_aware (default: benefit)
  --penalty VALUE               GREEDY_MOVE_PENALTY_COEFFICIENT (default: 0.2)
  --cpu-tau DURATION            GREEDY_CPU_SECONDS_SMOOTHING_TAU (default: 10m)
  --sample-interval SECONDS     Utilization sample interval (default: 30)
  --duration SECONDS            Run/sampling duration (default: 3600)
  --settle-seconds SECONDS      Wait after deploy before starting run (default: 120)
  --session NAME                tmux session name (default: kind-lab-RUN_NAME)
  --build-image                 Build cadence-kind-lab image first
  --create-cluster              Create/load kind cluster first
  --no-attach                   Do not attach to tmux after creating panes
  -h, --help                    Show this help

Environment variables with the same names can also be used for the greedy/profile settings.
EOF
}

safe_name() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9_.-' '-'
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
    --session)
      SESSION_NAME="$2"
      shift 2
      ;;
    --build-image)
      BUILD_IMAGE="true"
      shift
      ;;
    --create-cluster)
      CREATE_CLUSTER="true"
      shift
      ;;
    --no-attach)
      ATTACH="false"
      shift
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

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for this script" >&2
  exit 1
fi

SESSION_NAME="${SESSION_NAME:-kind-lab-$(safe_name "$RUN_NAME")}" 
RESULT_DIR="$ROOT/environment/kind-lab/results"
CSV_PATH="$RESULT_DIR/$RUN_NAME.csv"
LOG_PATH="$RESULT_DIR/$RUN_NAME.log"
mkdir -p "$RESULT_DIR"

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  echo "killing existing tmux session: $SESSION_NAME"
  tmux kill-session -t "$SESSION_NAME"
fi

if [[ "$BUILD_IMAGE" == "true" ]]; then
  "$ROOT/environment/kind-lab/scripts/build-image.sh"
fi
if [[ "$CREATE_CLUSTER" == "true" ]]; then
  "$ROOT/environment/kind-lab/scripts/create-cluster.sh"
fi

"$ROOT/environment/kind-lab/scripts/reset.sh"

MATCHING_HETEROGENEITY_PROFILE="$MATCHING_HETEROGENEITY_PROFILE" \
GREEDY_HETEROGENEITY_MODE="$GREEDY_HETEROGENEITY_MODE" \
GREEDY_MOVE_SCORING_MODE="$GREEDY_MOVE_SCORING_MODE" \
GREEDY_MOVE_PENALTY_COEFFICIENT="$GREEDY_MOVE_PENALTY_COEFFICIENT" \
GREEDY_CPU_SECONDS_SMOOTHING_TAU="$GREEDY_CPU_SECONDS_SMOOTHING_TAU" \
  "$ROOT/environment/kind-lab/scripts/deploy.sh" heterogeneous

"$ROOT/environment/kind-lab/scripts/deploy-observability.sh"

echo "settling for ${SETTLE_SECONDS}s before starting workload..."
sleep "$SETTLE_SECONDS"

kubectl delete job matching-lab -n "$NAMESPACE" --ignore-not-found
kubectl wait --for=delete job/matching-lab -n "$NAMESPACE" --timeout=60s >/dev/null 2>&1 || true

cat <<EOF
Starting tmux session: $SESSION_NAME
Run name:              $RUN_NAME
Scenario:              $SCENARIO
Profile:               $MATCHING_HETEROGENEITY_PROFILE
Heterogeneity mode:    $GREEDY_HETEROGENEITY_MODE
Move scoring mode:     $GREEDY_MOVE_SCORING_MODE
Move penalty:          $GREEDY_MOVE_PENALTY_COEFFICIENT
CPU smoothing tau:     $GREEDY_CPU_SECONDS_SMOOTHING_TAU
Utilization CSV:       $CSV_PATH
Matching log:          $LOG_PATH
EOF

sample_cmd="cd '$ROOT' && ./environment/kind-lab/scripts/sample-utilization.sh '$SAMPLE_INTERVAL_SECONDS' '$DURATION_SECONDS' '$CSV_PATH'; echo; echo 'sample-utilization finished; press enter'; read"
load_cmd="cd '$ROOT' && ./environment/kind-lab/scripts/run-load.sh '$SCENARIO' | tee '$LOG_PATH'; echo; echo 'run-load finished; press enter'; read"
watch_cmd="cd '$ROOT' && watch -n 5 'kubectl get pods,jobs -n $NAMESPACE; echo; kubectl top pods -n $NAMESPACE 2>/dev/null || true'"

grafana_cmd="kubectl -n '$NAMESPACE' port-forward svc/grafana 3000:3000; echo; echo 'grafana port-forward exited; press enter'; read"
prometheus_cmd="kubectl -n '$NAMESPACE' port-forward svc/prometheus 9090:9090; echo; echo 'prometheus port-forward exited; press enter'; read"

tmux new-session -d -s "$SESSION_NAME" -n run "$sample_cmd"
tmux split-window -h -t "$SESSION_NAME:run" "$load_cmd"
tmux split-window -v -t "$SESSION_NAME:run.1" "$watch_cmd"
tmux select-layout -t "$SESSION_NAME:run" tiled >/dev/null 2>&1 || true

tmux new-window -t "$SESSION_NAME" -n ports "$grafana_cmd"
tmux split-window -h -t "$SESSION_NAME:ports" "$prometheus_cmd"
tmux select-layout -t "$SESSION_NAME:ports" even-horizontal >/dev/null 2>&1 || true
tmux select-window -t "$SESSION_NAME:run" >/dev/null 2>&1 || true

if [[ "$ATTACH" == "true" ]]; then
  tmux attach -t "$SESSION_NAME"
else
  echo "attach with: tmux attach -t $SESSION_NAME"
fi
