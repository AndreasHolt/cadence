#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE="cadence-kind-lab"

kubectl apply -k "$ROOT/environment/kind-lab/k8s/observability"

kubectl rollout status deployment/prometheus -n "$NAMESPACE" --timeout=3m
kubectl rollout status deployment/grafana -n "$NAMESPACE" --timeout=3m

cat <<EOF
Observability is ready.

Grafana:
  kubectl -n $NAMESPACE port-forward svc/grafana 3000:3000
  http://localhost:3000

Prometheus:
  kubectl -n $NAMESPACE port-forward svc/prometheus 9090:9090
  http://localhost:9090
EOF
