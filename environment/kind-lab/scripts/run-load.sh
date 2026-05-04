#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE="cadence-kind-lab"
JOB_NAME="matching-lab"
SCENARIO="${1:-hotspot}"

kubectl delete job "$JOB_NAME" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
sed "s/value: hotspot/value: ${SCENARIO}/" "$ROOT/environment/kind-lab/k8s/load/matching-lab-job.yaml" | kubectl apply -f -

POD_NAME=""
for _ in $(seq 1 60); do
  POD_NAME="$(kubectl get pods -n "$NAMESPACE" -l "job-name=$JOB_NAME" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [ -n "$POD_NAME" ]; then
    break
  fi
  sleep 2
done

if [ -z "$POD_NAME" ]; then
  echo "timed out waiting for $JOB_NAME pod to be created" >&2
  kubectl get jobs,pods -n "$NAMESPACE"
  exit 1
fi

for _ in $(seq 1 60); do
  if kubectl logs -n "$NAMESPACE" "$POD_NAME" -c matching-lab --tail=1 >/dev/null 2>&1; then
    exec kubectl logs -n "$NAMESPACE" -f "$POD_NAME" -c matching-lab
  fi
  sleep 2
done

echo "timed out waiting for logs from pod $POD_NAME" >&2
kubectl describe pod "$POD_NAME" -n "$NAMESPACE"
exit 1
