#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STARTENV_SCRIPT="${STARTENV_SCRIPT:-$ROOT_DIR/startenv.bash}"
RUN_DIR="${RUN_DIR:-$ROOT_DIR/.run}"
START_LOG="${START_LOG:-$RUN_DIR/replay-feedback.log}"

RUN_SECONDS="${RUN_SECONDS:-120}"
STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-120}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"

CANARY_CSV="${CANARY_CSV:-21-12_00-06_fixed.csv}"
CANARY_EXECUTORS="${CANARY_EXECUTORS:-3}"
CANARY_REPLAY_SPEED="${CANARY_REPLAY_SPEED:-1.0}"
CANARY_EXECUTORS_FOR_CANARY=""

REPLAY_NAMESPACE="${REPLAY_NAMESPACE:-shard-distributor-replay}"
RUN_TESTS="${RUN_TESTS:-0}"
RESET_ETCD="${RESET_ETCD:-1}"
USE_CAFFEINATE="${USE_CAFFEINATE:-1}"
STOP_ON_EXIT="${STOP_ON_EXIT:-1}"

SD_METRICS_URL="${SD_METRICS_URL:-http://127.0.0.1:8004/metrics}"
CANARY_METRICS_URL="${CANARY_METRICS_URL:-http://127.0.0.1:9098/metrics}"
WAIT_CANARY_METRICS="${WAIT_CANARY_METRICS:-0}"
METRICS_NAMESPACE_LABEL="${METRICS_NAMESPACE_LABEL:-}"
METRICS_FILTER="${METRICS_FILTER:-}"

START_PID=""

usage() {
  cat <<'EOF'
Usage:
  ./scripts/replay_feedback.sh

Optional env vars:
  CANARY_CSV                 CSV replay file path (default: 21-12_00-06_fixed.csv)
  CANARY_EXECUTORS           replay executors (default: 3)
  CANARY_REPLAY_SPEED        replay speed multiplier (default: 1.0)
  RUN_SECONDS                run duration after startup (default: 120)
  RUN_TESTS                  set to 1 to run go tests first (default: 0)
  SKIP_BUILD                 set to 1 to skip make build in startenv (default: 0)
  SKIP_DOCKER_RESTART        set to 1 to skip prometheus/grafana restart in startenv (default: 0)
  RESET_ETCD                 set to 0 to skip rm -rf default.etcd (default: 1)
  USE_CAFFEINATE             set to 0 to skip caffeinate wrapper (default: 1)
  STOP_ON_EXIT               set to 0 to leave services running after summary (default: 1)
  REPLAY_NAMESPACE           namespace label filter in metrics (default: shard-distributor-replay)
  METRICS_NAMESPACE_LABEL    explicit namespace label value in metrics (default: sanitized REPLAY_NAMESPACE, e.g. shard_distributor_replay)
  METRICS_FILTER             full custom metrics filter override (default: namespace="<metrics label>")
  SD_METRICS_URL             shard distributor Prometheus endpoint
  CANARY_METRICS_URL         canary Prometheus endpoint
  WAIT_CANARY_METRICS        set to 1 to block until canary metrics endpoint is ready (default: 0)
EOF
}

log() {
  printf '[replay-feedback] %s\n' "$*"
}

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

cleanup() {
  if [[ -n "$START_PID" ]]; then
    kill "$START_PID" >/dev/null 2>&1 || true
    wait "$START_PID" 2>/dev/null || true
  fi

  if [[ "$STOP_ON_EXIT" == "1" ]]; then
    log "Stopping services"
    "$STARTENV_SCRIPT" stop >/dev/null 2>&1 || true
  fi
}

resolve_path() {
  local input="$1"
  if [[ "$input" = /* ]]; then
    printf '%s\n' "$input"
  else
    printf '%s/%s\n' "$ROOT_DIR" "$input"
  fi
}

wait_for_http() {
  local url="$1"
  local timeout="$2"
  local poll="$3"
  local waited=0

  while (( waited < timeout )); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$poll"
    waited=$((waited + poll))
  done

  return 1
}

capture_metrics() {
  local url="$1"
  local output="$2"
  curl -fsS "$url" >"$output"
}

read_metric_sum() {
  local metrics_file="$1"
  local metric_name="$2"
  local label_filter="${3:-}"

  awk -v metric="$metric_name" -v filter="$label_filter" '
    BEGIN {
      metric_prefix = "^" metric "(\\{|$)"
      found = 0
      sum = 0
    }
    $1 ~ metric_prefix {
      if (filter != "" && index($1, filter) == 0) {
        next
      }
      found = 1
      sum += $2
    }
    END {
      if (!found) {
        print 0
        exit 0
      }
      printf "%.12g\n", sum
    }
  ' "$metrics_file"
}

float_sub() {
  local a="$1"
  local b="$2"
  awk -v a="$a" -v b="$b" 'BEGIN { printf "%.6f\n", (a - b) }'
}

float_div() {
  local a="$1"
  local b="$2"
  awk -v a="$a" -v b="$b" 'BEGIN { if (b == 0) { print 0 } else { printf "%.6f\n", (a / b) } }'
}

print_summary() {
  local base="$1"
  local final="$2"

  local cycles_start moves_start
  local cycles_end moves_end
  local cycles_delta moves_delta
  local churn_per_cycle

  cycles_start="$(read_metric_sum "$base" "shard_distributor_load_balance_cycles" "$METRICS_FILTER")"
  moves_start="$(read_metric_sum "$base" "shard_distributor_load_balance_moves" "$METRICS_FILTER")"
  cycles_end="$(read_metric_sum "$final" "shard_distributor_load_balance_cycles" "$METRICS_FILTER")"
  moves_end="$(read_metric_sum "$final" "shard_distributor_load_balance_moves" "$METRICS_FILTER")"

  cycles_delta="$(float_sub "$cycles_end" "$cycles_start")"
  moves_delta="$(float_sub "$moves_end" "$moves_start")"
  churn_per_cycle="$(float_div "$moves_delta" "$cycles_delta")"

  local load_cv load_max smoothed_cv smoothed_max smoothed_missing smoothed_stale reassigned
  load_cv="$(read_metric_sum "$final" "shard_distributor_assignment_load_cv" "$METRICS_FILTER")"
  load_max="$(read_metric_sum "$final" "shard_distributor_assignment_load_max_over_mean" "$METRICS_FILTER")"
  smoothed_cv="$(read_metric_sum "$final" "shard_distributor_assignment_smoothed_load_cv" "$METRICS_FILTER")"
  smoothed_max="$(read_metric_sum "$final" "shard_distributor_assignment_smoothed_load_max_over_mean" "$METRICS_FILTER")"
  smoothed_missing="$(read_metric_sum "$final" "shard_distributor_assignment_smoothed_load_missing_ratio" "$METRICS_FILTER")"
  smoothed_stale="$(read_metric_sum "$final" "shard_distributor_assignment_smoothed_load_stale_ratio" "$METRICS_FILTER")"
  reassigned="$(read_metric_sum "$final" "shard_distributor_shard_assign_reassigned_shards" "$METRICS_FILTER")"

  printf '\n'
  printf '=== Replay feedback summary ===\n'
  printf 'namespace filter                : %s\n' "$METRICS_FILTER"
  printf 'replay csv                      : %s\n' "$CANARY_CSV_RESOLVED"
  printf 'replay executors                : %s\n' "$CANARY_EXECUTORS"
  printf 'replay speed                    : %s\n' "$CANARY_REPLAY_SPEED"
  printf 'run seconds                     : %s\n' "$RUN_SECONDS"
  printf '\n'
  printf 'load_balance_cycles delta       : %s\n' "$cycles_delta"
  printf 'load_balance_moves delta        : %s\n' "$moves_delta"
  printf 'moves per cycle (churn ratio)   : %s\n' "$churn_per_cycle"
  printf '\n'
  printf 'assignment_load_cv              : %s\n' "$load_cv"
  printf 'assignment_load_max_over_mean   : %s\n' "$load_max"
  printf 'smoothed_load_cv                : %s\n' "$smoothed_cv"
  printf 'smoothed_load_max_over_mean     : %s\n' "$smoothed_max"
  printf 'smoothed_load_missing_ratio     : %s\n' "$smoothed_missing"
  printf 'smoothed_load_stale_ratio       : %s\n' "$smoothed_stale"
  printf 'reassigned_shards gauge         : %s\n' "$reassigned"
  printf '================================\n'
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

require curl
require awk
require sleep

if [[ ! -x "$STARTENV_SCRIPT" ]]; then
  echo "startenv script not found or not executable: $STARTENV_SCRIPT" >&2
  exit 1
fi

if [[ -z "$METRICS_NAMESPACE_LABEL" ]]; then
 # Prometheus label values emitted by cadence metrics sanitize non-alphanumeric chars to underscores.
  METRICS_NAMESPACE_LABEL="$(printf '%s' "$REPLAY_NAMESPACE" | tr -c '[:alnum:]' '_')"
fi
if [[ -z "$METRICS_FILTER" ]]; then
  METRICS_FILTER="namespace=\"$METRICS_NAMESPACE_LABEL\""
fi

CANARY_CSV_RESOLVED="$(resolve_path "$CANARY_CSV")"
if [[ ! -f "$CANARY_CSV_RESOLVED" ]]; then
  echo "Replay CSV not found: $CANARY_CSV_RESOLVED" >&2
  exit 1
fi
if [[ ! "$CANARY_EXECUTORS" =~ ^[0-9]+$ ]]; then
  echo "CANARY_EXECUTORS must be a positive integer; got: $CANARY_EXECUTORS" >&2
  exit 1
fi
if (( CANARY_EXECUTORS < 1 )); then
  echo "CANARY_EXECUTORS must be >= 1; got: $CANARY_EXECUTORS" >&2
  exit 1
fi

# Replay canary currently registers one extra fixed executor (observed N+1 active executors).
# To keep CANARY_EXECUTORS as the user-facing "target active executors", we intentionally pass N-1.
# This keeps experiment scripts intuitive (CANARY_EXECUTORS=4 -> ~4 active executors in replay namespace).
if (( CANARY_EXECUTORS > 1 )); then
  CANARY_EXECUTORS_FOR_CANARY=$((CANARY_EXECUTORS - 1))
else
  CANARY_EXECUTORS_FOR_CANARY=1
fi

mkdir -p "$RUN_DIR"

trap cleanup EXIT

if [[ "$RUN_TESTS" == "1" ]]; then
  log "Running shard distributor tests"
  (
    cd "$ROOT_DIR/service/sharddistributor"
    go test ./...
  )
fi

log "Stopping previous environment"
"$STARTENV_SCRIPT" stop >/dev/null 2>&1 || true

if [[ "$RESET_ETCD" == "1" ]]; then
  log "Removing default.etcd"
  rm -rf "$ROOT_DIR/default.etcd"
fi

log "Starting environment (log: $START_LOG)"
: > "$START_LOG"

if [[ "$USE_CAFFEINATE" == "1" && -x "$(command -v caffeinate || true)" ]]; then
  (
    cd "$ROOT_DIR"
    CANARY_CSV="$CANARY_CSV_RESOLVED" \
      CANARY_EXECUTORS="$CANARY_EXECUTORS_FOR_CANARY" \
      CANARY_REPLAY_SPEED="$CANARY_REPLAY_SPEED" \
      caffeinate -dimsu "$STARTENV_SCRIPT" start
  ) >>"$START_LOG" 2>&1 &
else
  (
    cd "$ROOT_DIR"
    CANARY_CSV="$CANARY_CSV_RESOLVED" \
      CANARY_EXECUTORS="$CANARY_EXECUTORS_FOR_CANARY" \
      CANARY_REPLAY_SPEED="$CANARY_REPLAY_SPEED" \
      "$STARTENV_SCRIPT" start
  ) >>"$START_LOG" 2>&1 &
fi

START_PID=$!

log "Waiting for shard distributor metrics at $SD_METRICS_URL"
if ! wait_for_http "$SD_METRICS_URL" "$STARTUP_TIMEOUT_SECONDS" "$POLL_INTERVAL_SECONDS"; then
  echo "Timed out waiting for shard distributor metrics: $SD_METRICS_URL" >&2
  echo "See log: $START_LOG" >&2
  exit 1
fi

if [[ "$WAIT_CANARY_METRICS" == "1" ]]; then
  log "Waiting for canary metrics at $CANARY_METRICS_URL"
  if ! wait_for_http "$CANARY_METRICS_URL" "$STARTUP_TIMEOUT_SECONDS" "$POLL_INTERVAL_SECONDS"; then
    echo "Timed out waiting for canary metrics: $CANARY_METRICS_URL" >&2
    echo "See log: $START_LOG" >&2
    exit 1
  fi
else
  log "Skipping canary metrics readiness check (WAIT_CANARY_METRICS=$WAIT_CANARY_METRICS)"
fi

baseline_metrics="$(mktemp)"
final_metrics="$(mktemp)"

capture_metrics "$SD_METRICS_URL" "$baseline_metrics"
log "Captured baseline metrics; replay running for ${RUN_SECONDS}s"
sleep "$RUN_SECONDS"
capture_metrics "$SD_METRICS_URL" "$final_metrics"

print_summary "$baseline_metrics" "$final_metrics"

rm -f "$baseline_metrics" "$final_metrics"

if [[ "$STOP_ON_EXIT" == "0" ]]; then
  log "Leaving services running (STOP_ON_EXIT=0)"
fi
