#!/bin/bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-cadence-kind-lab}"
DEST_DIR="${1:-environment/kind-lab/results/cpu-debug}"
INTERVAL_SECONDS="${2:-30}"
DURATION_SECONDS="${3:-0}"

mkdir -p "$DEST_DIR"

copy_logs() {
  kubectl cp "$NAMESPACE/cadence-matching-a-0:/var/log/cadence-cpu-debug/executor-raw.csv" \
    "$DEST_DIR/executor-raw.csv" 2>/dev/null || true
  kubectl cp "$NAMESPACE/cadence-shard-distributor-0:/var/log/cadence-cpu-debug/observation.csv" \
    "$DEST_DIR/observation.csv" 2>/dev/null || true
}

copy_logs

if [[ "$DURATION_SECONDS" -le 0 ]]; then
  exit 0
fi

start_ts="$(date +%s)"
end_ts=$((start_ts + DURATION_SECONDS))
while [[ "$(date +%s)" -lt "$end_ts" ]]; do
  sleep "$INTERVAL_SECONDS"
  copy_logs
done
