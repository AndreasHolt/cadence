#!/bin/bash
set -euo pipefail

KIND_VERSION="${KIND_VERSION:-v0.30.0}"
CADENCE_REPO_URL="${CADENCE_REPO_URL:-https://github.com/AndreasHolt/cadence.git}"
CADENCE_BRANCH="${CADENCE_BRANCH:-greedy-load-balancing-clean-testbed}"
CADENCE_DIR="${CADENCE_DIR:-$HOME/cadence}"

if [[ -n "${SUDO_USER:-}" ]]; then
  TARGET_USER="$SUDO_USER"
else
  TARGET_USER="$USER"
fi

if [[ "$(id -u)" -eq 0 ]]; then
  SUDO=()
else
  SUDO=(sudo)
fi

require_linux() {
  if [[ "$(uname -s)" != "Linux" ]]; then
    echo "This installer is intended for Linux machines." >&2
    exit 1
  fi
}

require_ubuntu() {
  if [[ ! -r /etc/os-release ]]; then
    echo "Cannot find /etc/os-release." >&2
    exit 1
  fi

  # shellcheck disable=SC1091
  . /etc/os-release
  if [[ "${ID:-}" != "ubuntu" ]]; then
    echo "This installer is intended for Ubuntu. Detected ID=${ID:-unknown}." >&2
    exit 1
  fi
}

arch_suffix() {
  case "$(uname -m)" in
    x86_64)
      echo "amd64"
      ;;
    aarch64|arm64)
      echo "arm64"
      ;;
    *)
      echo "unsupported architecture: $(uname -m)" >&2
      exit 1
      ;;
  esac
}

install_base_packages() {
  "${SUDO[@]}" apt-get update
  "${SUDO[@]}" apt-get upgrade -y
  "${SUDO[@]}" apt-get install -y \
    ca-certificates \
    curl \
    git \
    gnupg \
    make \
    python3 \
    python3-matplotlib
}

install_docker() {
  if command -v docker >/dev/null 2>&1; then
    echo "docker already installed: $(docker --version)"
    return
  fi

  "${SUDO[@]}" install -m 0755 -d /etc/apt/keyrings
  "${SUDO[@]}" curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    -o /etc/apt/keyrings/docker.asc
  "${SUDO[@]}" chmod a+r /etc/apt/keyrings/docker.asc

  # shellcheck disable=SC1091
  . /etc/os-release
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
    | "${SUDO[@]}" tee /etc/apt/sources.list.d/docker.list >/dev/null

  "${SUDO[@]}" apt-get update
  "${SUDO[@]}" apt-get install -y \
    containerd.io \
    docker-buildx-plugin \
    docker-ce \
    docker-ce-cli \
    docker-compose-plugin
}

configure_docker_user() {
  "${SUDO[@]}" usermod -aG docker "$TARGET_USER"
  if command -v systemctl >/dev/null 2>&1; then
    "${SUDO[@]}" systemctl enable --now docker
  fi
}

install_kind() {
  if command -v kind >/dev/null 2>&1; then
    echo "kind already installed: $(kind --version)"
    return
  fi

  local arch
  arch="$(arch_suffix)"
  curl -fsSL -o /tmp/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${arch}"
  chmod +x /tmp/kind
  "${SUDO[@]}" mv /tmp/kind /usr/local/bin/kind
}

install_kubectl() {
  if command -v kubectl >/dev/null 2>&1; then
    echo "kubectl already installed: $(kubectl version --client=true 2>/dev/null | head -n 1)"
    return
  fi

  local arch
  local version
  arch="$(arch_suffix)"
  version="$(curl -fsSL https://dl.k8s.io/release/stable.txt)"
  curl -fsSL -o /tmp/kubectl "https://dl.k8s.io/release/${version}/bin/linux/${arch}/kubectl"
  chmod +x /tmp/kubectl
  "${SUDO[@]}" mv /tmp/kubectl /usr/local/bin/kubectl
}

clone_cadence_repo() {
  if [[ -d "$CADENCE_DIR/.git" ]]; then
    echo "Cadence repository already exists at $CADENCE_DIR"
    git -C "$CADENCE_DIR" fetch origin "$CADENCE_BRANCH"
    git -C "$CADENCE_DIR" checkout "$CADENCE_BRANCH"
    git -C "$CADENCE_DIR" pull --ff-only origin "$CADENCE_BRANCH"
  elif [[ -e "$CADENCE_DIR" ]]; then
    echo "CADENCE_DIR exists but is not a git repository: $CADENCE_DIR" >&2
    echo "Set CADENCE_DIR to another path or move the existing directory." >&2
    exit 1
  else
    git clone --branch "$CADENCE_BRANCH" --single-branch "$CADENCE_REPO_URL" "$CADENCE_DIR"
  fi

  git -C "$CADENCE_DIR" submodule update --init --recursive
}

print_summary() {
  echo
  echo "Installed dependencies:"
  docker --version || true
  kind --version || true
  kubectl version --client=true || true
  python3 - <<'PY' || true
import matplotlib
print("matplotlib", matplotlib.__version__)
PY
  echo
  echo "If this is the first time Docker was installed, refresh your group membership:"
  echo "  newgrp docker"
  echo
  echo "Then verify Docker:"
  echo "  docker run --rm hello-world"
  echo
  echo "Cadence checkout:"
  echo "  cd $CADENCE_DIR"
}

main() {
  require_linux
  require_ubuntu
  install_base_packages
  install_docker
  configure_docker_user
  install_kind
  install_kubectl
  clone_cadence_repo
  print_summary
}

main "$@"
