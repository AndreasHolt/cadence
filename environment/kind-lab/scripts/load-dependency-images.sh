#!/bin/bash
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER_NAME:-cadence-kind-lab}"
IMAGE_PLATFORM="${KIND_IMAGE_PLATFORM:-linux/amd64}"

IMAGES=(
  "cassandra:4.1.1"
  "bitnamilegacy/etcd:3.5.5"
)

cluster_exists() {
  kind get clusters | grep -qx "$CLUSTER_NAME"
}

load_image() {
  local image="$1"

  echo "Pulling $image for $IMAGE_PLATFORM"
  docker pull --platform "$IMAGE_PLATFORM" "$image"

  echo "Importing $image into kind cluster $CLUSTER_NAME"
  docker save "$image" \
    | docker exec -i "${CLUSTER_NAME}-control-plane" ctr -n k8s.io images import -
}

if ! cluster_exists; then
  echo "kind cluster $CLUSTER_NAME does not exist; create it before loading dependency images." >&2
  exit 1
fi

for image in "${IMAGES[@]}"; do
  load_image "$image"
done
