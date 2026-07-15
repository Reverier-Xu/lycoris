#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
NETWORK_LEFT="lycoris-e2e-left"
NETWORK_RIGHT="lycoris-e2e-right"

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker compose"
  EXEC="docker"
elif command -v docker-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker-compose"
  if command -v docker >/dev/null 2>&1; then
    EXEC="docker"
  else
    EXEC="podman"
    if [ -z "${DOCKER_HOST:-}" ] && [ -S "/run/user/$(id - u)/podman/podman.sock" ]; then
      export DOCKER_HOST="unix:///run/user/$(id - u)/podman/podman.sock"
    fi
  fi
elif command -v podman >/dev/null 2>&1 && command -v podman-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE="podman-compose"
  EXEC="podman"
else
  echo "error: neither docker compose nor podman-compose found" >&2
  exit 1
fi

cd "${SCRIPT_DIR}"

echo "=== generating certificates and configs ==="
./gen-certs.sh
./gen-configs.sh

echo "=== building container image ==="
${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" build

echo "=== starting cluster ==="
${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" down -v || true
${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" up -d

cleanup() {
  echo "=== cleaning up ==="
  ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" down -v || true
}
trap cleanup EXIT

lycoris_in() {
  local container="$1"
  shift
  ${EXEC} exec -i "${container}" lycoris "$@"
}

strip_ansi() {
  sed 's/\x1b\[[0-9;]*m//g'
}

wait_for_cluster() {
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if lycoris_in lycoris-e2e-node-0 cluster get nodes 2>/dev/null | strip_ansi | grep -q 'node-2'; then
      return 0
    fi
    echo "  waiting for cluster convergence..."
    sleep 1
  done
  echo "error: cluster did not converge in time" >&2
  return 1
}

node_count() {
  local container="$1"
  lycoris_in "${container}" cluster get nodes 2>/dev/null | strip_ansi | grep -c '^[^ ]' || true
}

has_node() {
  local container="$1"
  local node_id="$2"
  lycoris_in "${container}" cluster get nodes 2>/dev/null | strip_ansi | grep -q "^${node_id}$"
}

register_node() {
  local container="$1"
  local node_id="$2"
  local address="$3"
  echo "  registering ${node_id} via ${container}"
  lycoris_in "${container}" cluster register --id "${node_id}" --address "${address}"
}

echo "=== waiting for initial convergence ==="
wait_for_cluster

echo ""
echo "=== test 1: register a node and observe convergence ==="
register_node lycoris-e2e-node-0 alpha https://alpha:5001
sleep 3
if has_node lycoris-e2e-node-2 alpha; then
  echo "  ok: alpha propagated to node-2"
else
  echo "  fail: alpha did not propagate to node-2" >&2
  exit 1
fi

echo ""
echo "=== test 2: network partition ==="
echo "  disconnecting node-1 from both networks"
${EXEC} network disconnect "${NETWORK_LEFT}" lycoris-e2e-node-1
${EXEC} network disconnect "${NETWORK_RIGHT}" lycoris-e2e-node-1

register_node lycoris-e2e-node-0 beta https://beta:5001
sleep 3

if has_node lycoris-e2e-node-2 beta; then
  echo "  fail: beta crossed the partition to node-2" >&2
  ${EXEC} network connect "${NETWORK_LEFT}" lycoris-e2e-node-1 || true
  ${EXEC} network connect "${NETWORK_RIGHT}" lycoris-e2e-node-1 || true
  exit 1
else
  echo "  ok: beta did not cross the partition"
fi

echo "  reconnecting node-1"
${EXEC} network connect "${NETWORK_LEFT}" lycoris-e2e-node-1
${EXEC} network connect "${NETWORK_RIGHT}" lycoris-e2e-node-1
sleep 5

if has_node lycoris-e2e-node-2 beta; then
  echo "  ok: beta propagated to node-2 after reconnect"
else
  echo "  fail: beta did not propagate to node-2 after reconnect" >&2
  exit 1
fi

echo ""
echo "=== test 3: node failure and restart ==="
echo "  stopping node-2"
${EXEC} stop lycoris-e2e-node-2

register_node lycoris-e2e-node-0 gamma https://gamma:5001
sleep 3

if has_node lycoris-e2e-node-1 gamma; then
  echo "  ok: gamma reached node-1 while node-2 was down"
else
  echo "  fail: gamma did not reach node-1" >&2
  ${EXEC} start lycoris-e2e-node-2 || true
  exit 1
fi

echo "  restarting node-2"
${EXEC} start lycoris-e2e-node-2
sleep 5

if has_node lycoris-e2e-node-2 gamma; then
  echo "  ok: gamma propagated to node-2 after restart"
else
  echo "  fail: gamma did not propagate to node-2 after restart" >&2
  exit 1
fi

echo ""
echo "=== all e2e tests passed ==="
