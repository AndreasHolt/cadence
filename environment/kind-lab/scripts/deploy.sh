#!/bin/bash
set -euo pipefail

MODE="${1:-heterogeneous}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE="cadence-kind-lab"
GREEDY_HETEROGENEITY_MODE="${GREEDY_HETEROGENEITY_MODE:-off}"
GREEDY_MOVE_SCORING_MODE="${GREEDY_MOVE_SCORING_MODE:-benefit}"
GREEDY_MOVE_PENALTY_COEFFICIENT="${GREEDY_MOVE_PENALTY_COEFFICIENT:-0.2}"
MATCHING_HETEROGENEITY_PROFILE="${MATCHING_HETEROGENEITY_PROFILE:-equal_burn}"
MATCHING_BASE_BURN_ITERATIONS="${MATCHING_BASE_BURN_ITERATIONS:-15000000}"
MATCHING_FAST_BURN_ITERATIONS="${MATCHING_FAST_BURN_ITERATIONS:-10000000}"
MATCHING_SLOW_BURN_ITERATIONS="${MATCHING_SLOW_BURN_ITERATIONS:-20000000}"
GREEDY_CPU_SECONDS_SMOOTHING_TAU="${GREEDY_CPU_SECONDS_SMOOTHING_TAU:-10m}"

case "$MODE" in
  homogeneous|heterogeneous)
    ;;
  *)
    echo "usage: $0 [homogeneous|heterogeneous]" >&2
    exit 2
    ;;
esac

case "$GREEDY_HETEROGENEITY_MODE" in
  off|latency|cpu_seconds)
    ;;
  *)
    echo "GREEDY_HETEROGENEITY_MODE must be one of: off, latency, cpu_seconds" >&2
    exit 2
    ;;
esac

case "$GREEDY_MOVE_SCORING_MODE" in
  benefit|cost_aware)
    ;;
  *)
    echo "GREEDY_MOVE_SCORING_MODE must be one of: benefit, cost_aware" >&2
    exit 2
    ;;
esac

case "$MATCHING_HETEROGENEITY_PROFILE" in
  equal_burn)
    MATCHING_A_CPU="1"
    MATCHING_B_CPU="2"
    MATCHING_C_CPU="4"
    MATCHING_A_BURN="$MATCHING_BASE_BURN_ITERATIONS"
    MATCHING_B_BURN="$MATCHING_BASE_BURN_ITERATIONS"
    MATCHING_C_BURN="$MATCHING_BASE_BURN_ITERATIONS"
    ;;
  equal_cores)
    MATCHING_A_CPU="2"
    MATCHING_B_CPU="2"
    MATCHING_C_CPU="2"
    MATCHING_A_BURN="$MATCHING_FAST_BURN_ITERATIONS"
    MATCHING_B_BURN="$MATCHING_BASE_BURN_ITERATIONS"
    MATCHING_C_BURN="$MATCHING_SLOW_BURN_ITERATIONS"
    ;;
  mixed)
    MATCHING_A_CPU="1"
    MATCHING_B_CPU="2"
    MATCHING_C_CPU="4"
    MATCHING_A_BURN="$MATCHING_FAST_BURN_ITERATIONS"
    MATCHING_B_BURN="$MATCHING_BASE_BURN_ITERATIONS"
    MATCHING_C_BURN="$MATCHING_SLOW_BURN_ITERATIONS"
    ;;
  *)
    echo "MATCHING_HETEROGENEITY_PROFILE must be one of: equal_burn, equal_cores, mixed" >&2
    exit 2
    ;;
esac

configure_matching_executor() {
  local suffix="$1"
  local cpu="$2"
  local burn_iterations="$3"
  local statefulset="cadence-matching-${suffix}"

  kubectl set resources "statefulset/${statefulset}" -n "$NAMESPACE" -c matching \
    --requests="cpu=${cpu},memory=512Mi" \
    --limits="cpu=${cpu},memory=512Mi"
  kubectl set env "statefulset/${statefulset}" -n "$NAMESPACE" \
    "MATCHING_LAB_ADD_TASK_CPU_BURN_ITERATIONS=${burn_iterations}"
}

kubectl apply -k "$ROOT/environment/kind-lab/k8s/bootstrap"

tmp_config_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_config_dir"' EXIT
cp "$ROOT"/environment/kind-lab/k8s/bootstrap/files/* "$tmp_config_dir"/
awk -v heterogeneity_mode="$GREEDY_HETEROGENEITY_MODE" -v move_scoring_mode="$GREEDY_MOVE_SCORING_MODE" -v move_penalty_coefficient="$GREEDY_MOVE_PENALTY_COEFFICIENT" -v cpu_smoothing_tau="$GREEDY_CPU_SECONDS_SMOOTHING_TAU" '
  $0 == "shardDistributor.loadBalancingGreedy.heterogeneityMode:" {
    in_heterogeneity_key = 1
    print
    next
  }
  $0 == "shardDistributor.loadBalancingGreedy.moveScoringMode:" {
    in_move_scoring_key = 1
    print
    next
  }
  $0 == "shardDistributor.loadBalancingGreedy.movePenaltyCoefficient:" {
    in_move_penalty_coefficient_key = 1
    print
    next
  }
  $0 == "shardDistributor.loadBalancingGreedy.cpuSecondsSmoothingTau:" {
    in_cpu_smoothing_tau_key = 1
    print
    next
  }
  in_heterogeneity_key && $1 == "-" && $2 == "value:" {
    print "  - value: " heterogeneity_mode
    in_heterogeneity_key = 0
    next
  }
  in_move_scoring_key && $1 == "-" && $2 == "value:" {
    print "  - value: " move_scoring_mode
    in_move_scoring_key = 0
    next
  }
  in_move_penalty_coefficient_key && $1 == "-" && $2 == "value:" {
    print "  - value: " move_penalty_coefficient
    in_move_penalty_coefficient_key = 0
    next
  }
  in_cpu_smoothing_tau_key && $1 == "-" && $2 == "value:" {
    print "  - value: " cpu_smoothing_tau
    in_cpu_smoothing_tau_key = 0
    next
  }
  { print }
' "$tmp_config_dir/kind-lab-dynamic.yaml" > "$tmp_config_dir/kind-lab-dynamic.yaml.tmp"
mv "$tmp_config_dir/kind-lab-dynamic.yaml.tmp" "$tmp_config_dir/kind-lab-dynamic.yaml"
kubectl create configmap cadence-kind-lab-config -n "$NAMESPACE" \
  --from-file=kind-lab.yaml="$tmp_config_dir/kind-lab.yaml" \
  --from-file=kind-lab-dynamic.yaml="$tmp_config_dir/kind-lab-dynamic.yaml" \
  --from-file=hotspot.yaml="$tmp_config_dir/hotspot.yaml" \
  --from-file=trace-21-12.yaml="$tmp_config_dir/trace-21-12.yaml" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "greedy heterogeneity mode: $GREEDY_HETEROGENEITY_MODE"
echo "greedy move scoring mode: $GREEDY_MOVE_SCORING_MODE"
echo "greedy move penalty coefficient: $GREEDY_MOVE_PENALTY_COEFFICIENT"
echo "matching heterogeneity profile: $MATCHING_HETEROGENEITY_PROFILE"
echo "  cadence-matching-a: cpu=$MATCHING_A_CPU burn_iterations=$MATCHING_A_BURN"
echo "  cadence-matching-b: cpu=$MATCHING_B_CPU burn_iterations=$MATCHING_B_BURN"
echo "  cadence-matching-c: cpu=$MATCHING_C_CPU burn_iterations=$MATCHING_C_BURN"
echo "greedy cpu seconds smoothing tau: $GREEDY_CPU_SECONDS_SMOOTHING_TAU"

kubectl rollout status statefulset/cassandra -n "$NAMESPACE" --timeout=5m
kubectl rollout status statefulset/etcd -n "$NAMESPACE" --timeout=2m

kubectl delete job cadence-schema-setup -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
kubectl apply -f "$ROOT/environment/kind-lab/k8s/bootstrap/schema-job.yaml" -n "$NAMESPACE"
kubectl wait -n "$NAMESPACE" --for=condition=complete job/cadence-schema-setup --timeout=5m

kubectl apply -k "$ROOT/environment/kind-lab/k8s/apps/overlays/$MODE"
if [[ "$MODE" == "heterogeneous" ]]; then
  configure_matching_executor "a" "$MATCHING_A_CPU" "$MATCHING_A_BURN"
  configure_matching_executor "b" "$MATCHING_B_CPU" "$MATCHING_B_BURN"
  configure_matching_executor "c" "$MATCHING_C_CPU" "$MATCHING_C_BURN"
fi

kubectl rollout status statefulset/cadence-shard-distributor -n "$NAMESPACE" --timeout=5m
kubectl rollout status statefulset/cadence-frontend -n "$NAMESPACE" --timeout=5m
kubectl rollout status statefulset/cadence-history -n "$NAMESPACE" --timeout=5m
kubectl rollout status statefulset/cadence-matching-a -n "$NAMESPACE" --timeout=5m
kubectl rollout status statefulset/cadence-matching-b -n "$NAMESPACE" --timeout=5m
kubectl rollout status statefulset/cadence-matching-c -n "$NAMESPACE" --timeout=5m
