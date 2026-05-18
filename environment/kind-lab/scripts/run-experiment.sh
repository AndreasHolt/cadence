#!/bin/bash
set -euo pipefail

# Orchestrate N iterations of matching-lab runs for one or more move-scoring
# modes.  Between every iteration the namespace is fully reset and redeployed
# so each run starts from a clean slate.  Prometheus time-series are
# collected automatically after each run.
#
# Example:
#   MATCHING_HETEROGENEITY_PROFILE=homogeneous_zero_burn \
#   GREEDY_HETEROGENEITY_MODE=off \
#   GREEDY_MOVE_PENALTY_COEFFICIENT=0.2 \
#     ./environment/kind-lab/scripts/run-experiment.sh \
#       --iterations 3 \
#       --modes cost_aware,benefit \
#       --scenario trace-21-12 \
#       --output-dir environment/kind-lab/results/experiment

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE="cadence-kind-lab"

ITERATIONS=3
MODES="cost_aware,benefit"
SCENARIO="trace-21-12"
OUTPUT_DIR="$ROOT/environment/kind-lab/results/experiment"
PROMETHEUS_URL="http://localhost:9090"

usage() {
  cat <<EOF
Usage: ${0##*/} [OPTIONS]

Options:
  --iterations N          Number of iterations per mode (default: 3)
  --modes LIST            Comma-separated list of modes (default: cost_aware,benefit)
  --scenario NAME         Matching-lab scenario name (default: trace-21-12)
  --output-dir PATH       Directory for logs and prometheus data
  --prometheus-url URL    Prometheus URL (default: http://localhost:9090)
  --help                  Show this message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    --modes)
      MODES="$2"
      shift 2
      ;;
    --scenario)
      SCENARIO="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --prometheus-url)
      PROMETHEUS_URL="$2"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

# ------------------------------------------------------------------
# Preconditions
# ------------------------------------------------------------------
if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required" >&2
  exit 1
fi
if ! command -v kind >/dev/null 2>&1; then
  echo "kind is required" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
echo "Experiment output directory: $OUTPUT_DIR"

# ------------------------------------------------------------------
# One-time cluster + observability setup
# ------------------------------------------------------------------
echo "=== Creating cluster (if needed) ==="
"$ROOT/environment/kind-lab/scripts/create-cluster.sh"

echo "=== Deploying observability ==="
"$ROOT/environment/kind-lab/scripts/deploy-observability.sh"

# Port-forward Prometheus so collect-prometheus-run.py can reach it.
echo "=== Setting up Prometheus port-forward ==="
kubectl -n "$NAMESPACE" port-forward svc/prometheus 9090:9090 >/dev/null 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true' EXIT
sleep 3

if ! curl -fsS "$PROMETHEUS_URL/api/v1/status/config" >/dev/null 2>&1; then
  echo "WARNING: Prometheus does not appear to be reachable at $PROMETHEUS_URL" >&2
fi

# ------------------------------------------------------------------
# Main experiment loop
# ------------------------------------------------------------------
IFS=',' read -ra MODE_LIST <<< "$MODES"

for mode in "${MODE_LIST[@]}"; do
  mode_trimmed="${mode// /}"
  echo ""
  echo "============================================================"
  echo " Mode: $mode_trimmed"
  echo "============================================================"

  for i in $(seq 1 "$ITERATIONS"); do
    echo ""
    echo "--------------------------------------------------------"
    echo " Iteration $i / $ITERATIONS  (mode=$mode_trimmed)"
    echo " Started at: $(date -Iseconds)"
    echo "--------------------------------------------------------"

    run_label="${mode_trimmed}-iter${i}"
    log_file="$OUTPUT_DIR/${run_label}.log"

    # ---- Reset namespace -------------------------------------
    echo "  [$(date +%H:%M:%S)] Resetting namespace ..."
    "$ROOT/environment/kind-lab/scripts/reset.sh"

    # ---- Deploy cluster ---------------------------------------
    echo "  [$(date +%H:%M:%S)] Deploying cluster ..."
    GREEDY_MOVE_SCORING_MODE="$mode_trimmed" \
    MATCHING_HETEROGENEITY_PROFILE=homogeneous_zero_burn \
    GREEDY_HETEROGENEITY_MODE=off \
    GREEDY_MOVE_PENALTY_COEFFICIENT=0.2 \
      "$ROOT/environment/kind-lab/scripts/deploy.sh" heterogeneous

    # ---- Submit matching-lab job -----------------------------
    echo "  [$(date +%H:%M:%S)] Submitting matching-lab job (scenario=$SCENARIO) ..."
    kubectl delete job matching-lab -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
    sed "s/value: hotspot/value: ${SCENARIO}/" \
      "$ROOT/environment/kind-lab/k8s/load/matching-lab-job.yaml" \
      | kubectl apply -f -

    # Wait for pod to be scheduled
    pod_name=""
    for _ in $(seq 1 60); do
      pod_name="$(kubectl get pods -n "$NAMESPACE" -l job-name=matching-lab -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
      [[ -n "$pod_name" ]] && break
      sleep 2
    done
    if [[ -z "$pod_name" ]]; then
      echo "ERROR: timed out waiting for matching-lab pod" >&2
      exit 1
    fi
    echo "  [$(date +%H:%M:%S)] Pod created: $pod_name"

    # Wait for the pod to finish (Succeeded or Failed)
    echo "  [$(date +%H:%M:%S)] Waiting for job to finish ..."
    run_start_iso="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    job_succeeded=false
    for _ in $(seq 1 300); do
      phase="$(kubectl get pod -n "$NAMESPACE" "$pod_name" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
      if [[ "$phase" == "Succeeded" ]]; then
        job_succeeded=true
        break
      fi
      if [[ "$phase" == "Failed" ]]; then
        break
      fi
      sleep 2
    done

    # ---- Collect logs ----------------------------------------
    echo "  [$(date +%H:%M:%S)] Saving matching-lab log ..."
    kubectl logs -n "$NAMESPACE" "$pod_name" -c matching-lab > "$log_file" 2>&1 || true

    # Also try to pull the internal /tmp log file for redundancy
    internal_log="$(kubectl exec -n "$NAMESPACE" "$pod_name" -c matching-lab -- sh -c 'ls /tmp/matching-lab-*.log 2>/dev/null | tail -1' 2>/dev/null || true)"
    if [[ -n "$internal_log" ]]; then
      kubectl cp -n "$NAMESPACE" "$pod_name:$internal_log" "$OUTPUT_DIR/${run_label}-internal.log" -c matching-lab >/dev/null 2>&1 || true
    fi

    if [[ "$job_succeeded" != "true" ]]; then
      echo "  WARNING: job did not succeed; inspect $log_file" >&2
    fi

    # ---- Collect Prometheus data -----------------------------
    echo "  [$(date +%H:%M:%S)] Collecting Prometheus data ..."
    python3 "$ROOT/environment/kind-lab/scripts/collect-prometheus-run.py" \
      --run "$run_label" \
      --matching-log "$log_file" \
      --start "$run_start_iso" \
      --prometheus-url "$PROMETHEUS_URL" \
      --output-dir "$OUTPUT_DIR/prometheus" \
      2> "$OUTPUT_DIR/${run_label}-prometheus.err" || {
        echo "  WARNING: Prometheus collection failed; see ${run_label}-prometheus.err" >&2
      }

    # ---- Clean up --------------------------------------------
    echo "  [$(date +%H:%M:%S)] Cleaning up job ..."
    kubectl delete job matching-lab -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true

    echo "  [$(date +%H:%M:%S)] Done. Log: $log_file"
  done
done

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
echo ""
echo "============================================================"
echo "Experiment complete."
echo "Output directory: $OUTPUT_DIR"
echo ""
echo "Files generated:"
for f in "$OUTPUT_DIR"/*.log; do
  [[ -e "$f" ]] || continue
  echo "  $(basename "$f")"
done
echo "============================================================"
