#!/bin/bash
set -euo pipefail

NAMESPACE="cadence-kind-lab"

kubectl delete namespace "$NAMESPACE" --ignore-not-found
kubectl wait --for=delete namespace/"$NAMESPACE" --timeout=120s >/dev/null 2>&1 || true
