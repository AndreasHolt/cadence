#!/bin/bash
set -euo pipefail

MODE="${1:-heterogeneous}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE="cadence-kind-lab"
GREEDY_HETEROGENEITY_MODE="${GREEDY_HETEROGENEITY_MODE:-off}"

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

kubectl apply -k "$ROOT/environment/kind-lab/k8s/bootstrap"

tmp_config_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_config_dir"' EXIT
cp "$ROOT"/environment/kind-lab/k8s/bootstrap/files/* "$tmp_config_dir"/
awk -v mode="$GREEDY_HETEROGENEITY_MODE" '
  $0 == "shardDistributor.loadBalancingGreedy.heterogeneityMode:" {
    in_key = 1
    print
    next
  }
  in_key && $1 == "-" && $2 == "value:" {
    print "  - value: " mode
    in_key = 0
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

kubectl rollout status statefulset/cassandra -n "$NAMESPACE" --timeout=5m
kubectl rollout status statefulset/etcd -n "$NAMESPACE" --timeout=2m

kubectl delete job cadence-schema-setup -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
kubectl apply -f "$ROOT/environment/kind-lab/k8s/bootstrap/schema-job.yaml" -n "$NAMESPACE"
kubectl wait -n "$NAMESPACE" --for=condition=complete job/cadence-schema-setup --timeout=5m

kubectl apply -k "$ROOT/environment/kind-lab/k8s/apps/overlays/$MODE"

kubectl rollout status statefulset/cadence-shard-distributor -n "$NAMESPACE" --timeout=5m
kubectl rollout status statefulset/cadence-frontend -n "$NAMESPACE" --timeout=5m
kubectl rollout status statefulset/cadence-history -n "$NAMESPACE" --timeout=5m
kubectl rollout status statefulset/cadence-matching-a -n "$NAMESPACE" --timeout=5m
kubectl rollout status statefulset/cadence-matching-b -n "$NAMESPACE" --timeout=5m
kubectl rollout status statefulset/cadence-matching-c -n "$NAMESPACE" --timeout=5m
