#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/../../.."
docker build --network=host -t cadence-kind-lab:dev --target cadence-server .

if command -v kind >/dev/null 2>&1 && kind get clusters | grep -qx cadence-kind-lab; then
  kind load docker-image cadence-kind-lab:dev --name cadence-kind-lab
fi
