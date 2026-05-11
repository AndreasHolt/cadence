#!/bin/bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-cadence-kind-lab}"
INTERVAL_SECONDS="${1:-10}"
DURATION_SECONDS="${2:-0}"
OUTPUT="${3:-environment/kind-lab/results/utilization.csv}"

mkdir -p "$(dirname "$OUTPUT")"

BASE_PODS=(
  cadence-frontend-0
  cadence-history-0
  cadence-matching-a-0
  cadence-matching-b-0
  cadence-matching-c-0
  cadence-shard-distributor-0
  cassandra-0
  etcd-0
)

now_seconds() {
  date +%s
}

matching_lab_pod() {
  kubectl get pods -n "$NAMESPACE" -l job-name=matching-lab \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

read_pod_cgroup() {
  local pod="$1"
  local sampled_at_usec
  sampled_at_usec="$(date +%s%6N)"
  kubectl exec -n "$NAMESPACE" "$pod" -- sh -c '
    usage=$(awk "/usage_usec/ {print \$2}" /sys/fs/cgroup/cpu.stat)
    throttled=$(awk "/throttled_usec/ {print \$2}" /sys/fs/cgroup/cpu.stat)
    nr_throttled=$(awk "/nr_throttled/ {print \$2}" /sys/fs/cgroup/cpu.stat)
    mem=$(cat /sys/fs/cgroup/memory.current)
    max=$(cat /sys/fs/cgroup/memory.max)
    printf "%s,%s,%s,%s,%s" "$usage" "$throttled" "$nr_throttled" "$mem" "$max"
  ' | awk -v sampled_at_usec="$sampled_at_usec" '{print sampled_at_usec "," $0}'
}

collect_snapshot() {
  local file="$1"
  : > "$file"

  local pods=("${BASE_PODS[@]}")
  local lab_pod
  lab_pod="$(matching_lab_pod)"
  if [[ -n "$lab_pod" ]]; then
    pods+=("$lab_pod")
  fi

  for pod in "${pods[@]}"; do
    if kubectl get pod -n "$NAMESPACE" "$pod" >/dev/null 2>&1; then
      printf "%s,%s\n" "$pod" "$(read_pod_cgroup "$pod")" >> "$file"
    fi
  done
}

write_header_if_needed() {
  if [[ ! -s "$OUTPUT" ]]; then
    echo "timestamp,pod,cpu_cores,throttled_cores,throttled_events,memory_mib,memory_max" > "$OUTPUT"
  fi
}

write_delta_rows() {
  local start_file="$1"
  local end_file="$2"
  local timestamp="$3"
  local interval="$4"

  awk -F, -v ts="$timestamp" -v interval="$interval" '
    NR == FNR {
      sampled_at_usec[$1] = $2
      usage[$1] = $3
      throttled[$1] = $4
      nr_throttled[$1] = $5
      next
    }
    ($1 in usage) {
      elapsed_seconds = ($2 - sampled_at_usec[$1]) / 1000000
      usage_delta = $3 - usage[$1]
      throttled_delta = $4 - throttled[$1]
      throttled_events_delta = $5 - nr_throttled[$1]
      if (elapsed_seconds <= 0 || usage_delta < 0 || throttled_delta < 0 || throttled_events_delta < 0) {
        next
      }
      cpu_cores = usage_delta / (elapsed_seconds * 1000000)
      throttled_cores = throttled_delta / (elapsed_seconds * 1000000)
      throttled_events = throttled_events_delta
      memory_mib = $6 / 1048576
      printf "%s,%s,%.4f,%.4f,%d,%.2f,%s\n", ts, $1, cpu_cores, throttled_cores, throttled_events, memory_mib, $7
    }
  ' "$start_file" "$end_file" >> "$OUTPUT"
}

write_header_if_needed
start_time="$(now_seconds)"

while true; do
  before="$(mktemp)"
  after="$(mktemp)"
  collect_snapshot "$before"
  sleep "$INTERVAL_SECONDS"
  collect_snapshot "$after"
  write_delta_rows "$before" "$after" "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$INTERVAL_SECONDS"
  rm -f "$before" "$after"

  if [[ "$DURATION_SECONDS" != "0" ]] && (( "$(now_seconds)" - start_time >= DURATION_SECONDS )); then
    break
  fi
done
