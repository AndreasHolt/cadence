#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
CLUSTER_NAME="cadence-kind-lab"

if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
  kind create cluster --config "$ROOT/environment/kind-lab/kind-cluster.yaml"
fi

"$ROOT/environment/kind-lab/scripts/load-dependency-images.sh"
kind load docker-image cadence-kind-lab:dev --name "$CLUSTER_NAME"
