#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/../../.."

docker_args=(--network=host -t cadence-kind-lab:dev --target cadence-server)
if [[ "${CADENCE_KIND_LAB_DOCKER_NO_CACHE:-}" == "1" || "${CADENCE_KIND_LAB_DOCKER_NO_CACHE:-}" == "true" ]]; then
  docker_args=(--no-cache "${docker_args[@]}")
fi

docker build "${docker_args[@]}" .

if command -v kind >/dev/null 2>&1 && kind get clusters | grep -qx cadence-kind-lab; then
  kind load docker-image cadence-kind-lab:dev --name cadence-kind-lab
fi
