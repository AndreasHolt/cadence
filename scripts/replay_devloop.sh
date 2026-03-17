#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPLAY_SCRIPT="$ROOT_DIR/scripts/replay_feedback.sh"
ETCD_TEST_SCRIPT="${ETCD_TEST_SCRIPT:-$HOME/.codex/skills/public/cadence-etcd-test-suite/scripts/run_sharddistributor_etcd_tests.sh}"

RUN_TESTS="${RUN_TESTS:-0}"
TEST_TARGET="${TEST_TARGET:-./...}"
GO_TEST_EXTRA_ARGS="${GO_TEST_EXTRA_ARGS:-}"

CANARY_CSV="${CANARY_CSV:-21-12_00-06_fixed.csv}"
CANARY_EXECUTORS="${CANARY_EXECUTORS:-3}"
CANARY_REPLAY_SPEED="${CANARY_REPLAY_SPEED:-1.0}"

RUN_SECONDS="${RUN_SECONDS:-45}"
SKIP_BUILD="${SKIP_BUILD:-1}"
SKIP_DOCKER_RESTART="${SKIP_DOCKER_RESTART:-1}"
RESET_ETCD="${RESET_ETCD:-1}"
STOP_ON_EXIT="${STOP_ON_EXIT:-1}"
USE_CAFFEINATE="${USE_CAFFEINATE:-1}"
SHOW_REPORT="${SHOW_REPORT:-1}"
REPORT_RECENT="${REPORT_RECENT:-3}"
REPORT_SCRIPT="${REPORT_SCRIPT:-$ROOT_DIR/scripts/replay_runs_report.py}"

RUNS_ROOT="${RUNS_ROOT:-$ROOT_DIR/plots/devloop_runs}"
RUN_LABEL="${RUN_LABEL:-}"

RUN_ID=""
RUN_TITLE=""
RUN_DIR=""
RAW_LOG=""
SUMMARY_FILE=""
INDEX_FILE=""

usage() {
  cat <<'EOF'
Usage:
  ./scripts/replay_devloop.sh --label <slug>

This runs a tight local loop:
1) optional Go tests
2) replay run
3) printed load-balance/churn summary
4) persisted run artifacts + appended index row

Optional env vars:
  RUN_TESTS              1 to run tests before replay (default: 0)
  TEST_TARGET            go test target when RUN_TESTS=1 (default: ./...)
  GO_TEST_EXTRA_ARGS     extra go test args (example: "-run TestSubscribe -v")
  RUN_SECONDS            replay sample window seconds (default: 45)
  SKIP_BUILD             1 skips make build in startenv (default: 1)
  SKIP_DOCKER_RESTART    1 skips Prom/Grafana restart (default: 1)
  RESET_ETCD             1 clears default.etcd (default: 1)
  STOP_ON_EXIT           1 stops services after summary (default: 1)
  CANARY_CSV             replay CSV (default: 21-12_00-06_fixed.csv)
  CANARY_EXECUTORS       replay executors (default: 3)
  CANARY_REPLAY_SPEED    replay speed (default: 1.0)
  RUNS_ROOT              output root for run artifacts (default: plots/devloop_runs)
  RUN_LABEL              same as --label (lowercase slug)
  SHOW_REPORT            1 prints comparison report after run (default: 1)
  REPORT_RECENT          number of prior runs in report (default: 3)
  REPORT_SCRIPT          report script path (default: scripts/replay_runs_report.py)

Run title format (auto-generated and enforced):
  replay__<label>__sp<speed>__ex<executors>

Label format:
  lowercase letters, digits, and hyphens only
  regex: ^[a-z0-9]+(-[a-z0-9]+)*$
EOF
}

log() {
  printf '[replay-devloop] %s\n' "$*"
}

csv_quote() {
  local value="$1"
  value="${value//\"/\"\"}"
  printf '"%s"' "$value"
}

extract_metric() {
  local key="$1"
  awk -v key="$key" '
    index($0, key) == 1 {
      line = $0
      sub("^[^:]*:[[:space:]]*", "", line)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
      value = line
    }
    END {
      if (value == "") {
        print "NA"
      } else {
        print value
      }
    }
  ' "$SUMMARY_FILE"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --label)
        RUN_LABEL="${2:-}"
        shift 2
        ;;
      --runs-root)
        RUNS_ROOT="${2:-}"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown argument: $1" >&2
        usage >&2
        exit 2
        ;;
    esac
  done
}

validate_label() {
  if [[ -z "$RUN_LABEL" ]]; then
    echo "RUN_LABEL/--label is required." >&2
    usage >&2
    exit 2
  fi
  if [[ ! "$RUN_LABEL" =~ ^[a-z0-9]+(-[a-z0-9]+)*$ ]]; then
    echo "Invalid label '$RUN_LABEL'. Must match: ^[a-z0-9]+(-[a-z0-9]+)*$" >&2
    exit 2
  fi
}

prepare_run_paths() {
  local speed_text
  speed_text="$(printf "%s" "$CANARY_REPLAY_SPEED" | tr '[:upper:]' '[:lower:]')"
  RUN_TITLE="replay__${RUN_LABEL}__sp${speed_text}__ex${CANARY_EXECUTORS}"
  RUN_ID="$(date -u +%Y%m%d_%H%M%S)"

  RUN_DIR="$RUNS_ROOT/${RUN_ID}__${RUN_TITLE}"
  RAW_LOG="$RUN_DIR/raw.log"
  SUMMARY_FILE="$RUN_DIR/summary.txt"
  INDEX_FILE="$RUNS_ROOT/index.csv"

  mkdir -p "$RUN_DIR"
}

write_run_config() {
  local git_sha
  git_sha="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")"
  cat > "$RUN_DIR/run_config.env" <<EOF
RUN_ID=$RUN_ID
RUN_TITLE=$RUN_TITLE
RUN_LABEL=$RUN_LABEL
TIMESTAMP_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)
GIT_SHA=$git_sha
CANARY_CSV=$CANARY_CSV
CANARY_EXECUTORS=$CANARY_EXECUTORS
CANARY_REPLAY_SPEED=$CANARY_REPLAY_SPEED
RUN_SECONDS=$RUN_SECONDS
RUN_TESTS=$RUN_TESTS
TEST_TARGET=$TEST_TARGET
GO_TEST_EXTRA_ARGS=$GO_TEST_EXTRA_ARGS
SKIP_BUILD=$SKIP_BUILD
SKIP_DOCKER_RESTART=$SKIP_DOCKER_RESTART
RESET_ETCD=$RESET_ETCD
STOP_ON_EXIT=$STOP_ON_EXIT
USE_CAFFEINATE=$USE_CAFFEINATE
EOF
}

run_optional_tests() {
  if [[ "$RUN_TESTS" != "1" ]]; then
    return 0
  fi

  log "Running tests before replay"
  if [[ -x "$ETCD_TEST_SCRIPT" ]]; then
    TEST_TARGET="$TEST_TARGET" \
    GO_TEST_EXTRA_ARGS="$GO_TEST_EXTRA_ARGS" \
      "$ETCD_TEST_SCRIPT" --repo "$ROOT_DIR" 2>&1 | tee "$RUN_DIR/tests.log"
  else
    # Fall back to direct go test if the skill script is unavailable.
    (
      cd "$ROOT_DIR/service/sharddistributor"
      # shellcheck disable=SC2206
      extra_args=( $GO_TEST_EXTRA_ARGS )
      go test "$TEST_TARGET" -count=1 "${extra_args[@]}"
    ) 2>&1 | tee "$RUN_DIR/tests.log"
  fi
}

append_index_row() {
  local cycles_delta moves_delta moves_per_cycle
  local assignment_cv assignment_max
  local smoothed_cv smoothed_max smoothed_missing smoothed_stale
  local reassigned git_sha status

  cycles_delta="$(extract_metric "load_balance_cycles delta")"
  moves_delta="$(extract_metric "load_balance_moves delta")"
  moves_per_cycle="$(extract_metric "moves per cycle (churn ratio)")"
  assignment_cv="$(extract_metric "assignment_load_cv")"
  assignment_max="$(extract_metric "assignment_load_max_over_mean")"
  smoothed_cv="$(extract_metric "smoothed_load_cv")"
  smoothed_max="$(extract_metric "smoothed_load_max_over_mean")"
  smoothed_missing="$(extract_metric "smoothed_load_missing_ratio")"
  smoothed_stale="$(extract_metric "smoothed_load_stale_ratio")"
  reassigned="$(extract_metric "reassigned_shards gauge")"
  git_sha="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")"
  status="$1"

  if [[ ! -f "$INDEX_FILE" ]]; then
    cat > "$INDEX_FILE" <<'EOF'
"timestamp_utc","run_id","run_title","run_label","status","git_sha","csv","executors","replay_speed","run_seconds","run_tests","test_target","go_test_extra_args","skip_build","skip_docker_restart","cycles_delta","moves_delta","moves_per_cycle","assignment_load_cv","assignment_load_max_over_mean","smoothed_load_cv","smoothed_load_max_over_mean","smoothed_load_missing_ratio","smoothed_load_stale_ratio","reassigned_shards"
EOF
  fi

  {
    csv_quote "$(date -u +%Y-%m-%dT%H:%M:%SZ)"; printf ','
    csv_quote "$RUN_ID"; printf ','
    csv_quote "$RUN_TITLE"; printf ','
    csv_quote "$RUN_LABEL"; printf ','
    csv_quote "$status"; printf ','
    csv_quote "$git_sha"; printf ','
    csv_quote "$CANARY_CSV"; printf ','
    csv_quote "$CANARY_EXECUTORS"; printf ','
    csv_quote "$CANARY_REPLAY_SPEED"; printf ','
    csv_quote "$RUN_SECONDS"; printf ','
    csv_quote "$RUN_TESTS"; printf ','
    csv_quote "$TEST_TARGET"; printf ','
    csv_quote "$GO_TEST_EXTRA_ARGS"; printf ','
    csv_quote "$SKIP_BUILD"; printf ','
    csv_quote "$SKIP_DOCKER_RESTART"; printf ','
    csv_quote "$cycles_delta"; printf ','
    csv_quote "$moves_delta"; printf ','
    csv_quote "$moves_per_cycle"; printf ','
    csv_quote "$assignment_cv"; printf ','
    csv_quote "$assignment_max"; printf ','
    csv_quote "$smoothed_cv"; printf ','
    csv_quote "$smoothed_max"; printf ','
    csv_quote "$smoothed_missing"; printf ','
    csv_quote "$smoothed_stale"; printf ','
    csv_quote "$reassigned"; printf '\n'
  } >> "$INDEX_FILE"
}

parse_args "$@"
validate_label

if [[ ! -x "$REPLAY_SCRIPT" ]]; then
  echo "Missing replay script: $REPLAY_SCRIPT" >&2
  exit 1
fi

if [[ "$SKIP_BUILD" == "1" ]]; then
  if [[ ! -x "$ROOT_DIR/cadence-server" || ! -x "$ROOT_DIR/sharddistributor-canary" ]]; then
    log "Built binaries not found; forcing build for this run"
    SKIP_BUILD=0
  fi
fi

prepare_run_paths
write_run_config

log "Running replay feedback"
log "Run title: $RUN_TITLE"
log "Artifacts dir: $RUN_DIR"
log "Settings: RUN_SECONDS=$RUN_SECONDS SKIP_BUILD=$SKIP_BUILD SKIP_DOCKER_RESTART=$SKIP_DOCKER_RESTART CANARY_REPLAY_SPEED=$CANARY_REPLAY_SPEED"

run_optional_tests

set +e
SKIP_BUILD="$SKIP_BUILD" \
SKIP_DOCKER_RESTART="$SKIP_DOCKER_RESTART" \
RUN_SECONDS="$RUN_SECONDS" \
RESET_ETCD="$RESET_ETCD" \
STOP_ON_EXIT="$STOP_ON_EXIT" \
USE_CAFFEINATE="$USE_CAFFEINATE" \
CANARY_CSV="$CANARY_CSV" \
CANARY_EXECUTORS="$CANARY_EXECUTORS" \
CANARY_REPLAY_SPEED="$CANARY_REPLAY_SPEED" \
"$REPLAY_SCRIPT" 2>&1 | tee "$RAW_LOG"
REPLAY_EXIT="${PIPESTATUS[0]}"
set -e

awk '
  /=== Replay feedback summary ===/ {in_summary=1}
  in_summary {print}
  /================================/ && in_summary {exit}
' "$RAW_LOG" > "$SUMMARY_FILE"

if [[ -s "$SUMMARY_FILE" ]]; then
  append_index_row "ok"
else
  printf 'summary_not_found\n' > "$SUMMARY_FILE"
  append_index_row "summary_missing"
fi

log "Saved raw log: $RAW_LOG"
log "Saved summary: $SUMMARY_FILE"
log "Updated index: $INDEX_FILE"

if [[ "$SHOW_REPORT" == "1" ]]; then
  if command -v python3 >/dev/null 2>&1 && [[ -f "$REPORT_SCRIPT" ]]; then
    printf '\n'
    log "Comparison report"
    python3 "$REPORT_SCRIPT" \
      --runs-root "$RUNS_ROOT" \
      --run-id "$RUN_ID" \
      --recent "$REPORT_RECENT" || true
  else
    log "Skipping report: python3 or report script missing"
  fi
fi

exit "$REPLAY_EXIT"
