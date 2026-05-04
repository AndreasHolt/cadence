#!/bin/bash
set -euo pipefail

MODE="${1:-heterogeneous}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE="cadence-kind-lab"

case "$MODE" in
  homogeneous|heterogeneous)
    ;;
  *)
    echo "usage: $0 [homogeneous|heterogeneous]" >&2
    exit 2
    ;;
esac

kubectl apply -k "$ROOT/environment/kind-lab/k8s/bootstrap"

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
