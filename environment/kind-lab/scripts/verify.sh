#!/bin/bash
set -euo pipefail

NAMESPACE="cadence-kind-lab"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
GOCACHE_DIR="$ROOT_DIR/.build/go-cache"

kubectl get pods -n "$NAMESPACE" -o wide

echo
for pod in cadence-matching-a-0 cadence-matching-b-0 cadence-matching-c-0; do
  echo "== $pod resources =="
  kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.spec.containers[0].resources}'
  echo
  echo "== $pod gomaxprocs =="
  kubectl exec "$pod" -n "$NAMESPACE" -- sh -c 'curl -fsS localhost:9090/metrics | grep -i maxproc || true'
  echo
 done

echo "== shard distributor executor state =="
mkdir -p "$GOCACHE_DIR"
kubectl exec -n "$NAMESPACE" etcd-0 -- sh -lc 'ETCDCTL_API=3 /opt/bitnami/etcd/bin/etcdctl --endpoints=http://127.0.0.1:2379 get store/cadence-matching-kind-lab/executors/ --prefix -w json' \
  | GOCACHE="$GOCACHE_DIR" go run "$ROOT_DIR/cmd/tools/shardlabstate" --namespace cadence-matching-kind-lab --prefix store
