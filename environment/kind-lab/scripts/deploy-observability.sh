#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE="cadence-kind-lab"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-cadence-kind-lab}"
KIND_NODE="${KIND_NODE:-${KIND_CLUSTER_NAME}-control-plane}"

import_deploy_image() {
  local deploy="$1"
  local image

  image="$(kubectl get deploy "$deploy" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].image}')"

  echo "Importing image for $deploy: $image"
  docker pull "$image"
  docker save "$image" | docker exec -i "$KIND_NODE" ctr -n k8s.io images import -
}

existing_dashboard="$(mktemp)"
trap 'rm -f "$existing_dashboard"' EXIT
if kubectl get configmap kind-lab-grafana-dashboard-experiments -n "$NAMESPACE" >/dev/null 2>&1; then
  kubectl get configmap kind-lab-grafana-dashboard-experiments -n "$NAMESPACE" \
    -o jsonpath='{.data.cadence-experiment-overview\.json}' >"$existing_dashboard" || true
fi

kubectl apply -k "$ROOT/environment/kind-lab/k8s/observability"

if [[ -s "$existing_dashboard" ]]; then
  kubectl create configmap kind-lab-grafana-dashboard-experiments -n "$NAMESPACE" \
    --from-file=cadence-experiment-overview.json="$existing_dashboard" \
    --dry-run=client -o yaml | kubectl apply -f -
fi

kubectl patch deploy grafana prometheus -n "$NAMESPACE" \
  --type='json' \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]' || true

import_deploy_image prometheus
import_deploy_image grafana

kubectl rollout restart deployment/prometheus deployment/grafana -n "$NAMESPACE"

kubectl rollout status deployment/prometheus -n "$NAMESPACE" --timeout=3m
kubectl rollout status deployment/grafana -n "$NAMESPACE" --timeout=3m

cat <<EOF
Observability is ready.

Grafana:
  kubectl -n $NAMESPACE port-forward svc/grafana 3000:3000
  http://localhost:3000

Dashboard (folder: Cadence Kind Lab):
  - Cadence Matching Lab Experiments (/d/cadence-kind-lab-experiments)
    Run config banner is preserved from the active deployment when present.

Prometheus:
  kubectl -n $NAMESPACE port-forward svc/prometheus 9090:9090
  http://localhost:9090
EOF
