#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
E2E_DIR="$(mktemp -d)"
NETWORK="lycoris-shell-e2e"
IMAGE="lycoris-shell-test:latest"
NODE_COUNT=3

cleanup() {
  echo "cleaning up..."
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    podman rm -f "node-${i}" >/dev/null 2>&1 || true
  done
  podman network rm -f "${NETWORK}" >/dev/null 2>&1 || true
  rm -rf "${E2E_DIR}"
}
trap cleanup EXIT

run_in_node() {
  local node_id="$1"
  shift
  podman exec "node-${node_id}" "$@"
}

wait_for_file() {
  local path="$1"
  local timeout_secs="${2:-15}"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    [[ -f "${path}" ]] && return 0
    sleep 0.2
  done
  [[ -f "${path}" ]]
}

# Poll until the daemon inside a container answers CLI queries again (used
# after container restarts).
wait_until_ready() {
  local node="$1"
  local timeout_secs="${2:-30}"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    if run_in_node "${node}" lycoris cluster get nodes 2>/dev/null | grep -q "node-${node}"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Poll until the observer sees every node in the cluster.
wait_until_nodes_visible() {
  local observer="$1"
  local timeout_secs="${2:-30}"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    local output visible=true
    output="$(run_in_node "${observer}" lycoris cluster get nodes 2>/dev/null || true)"
    for i in $(seq 0 $((NODE_COUNT - 1))); do
      if ! echo "${output}" | grep -q "node-${i}"; then
        visible=false
        break
      fi
    done
    ${visible} && return 0
    sleep 1
  done
  return 1
}

# Poll until a node is no longer reported active by the observer.
wait_until_not_active() {
  local observer="$1"
  local node="$2"
  local timeout_secs="${3:-30}"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    if ! run_in_node "${observer}" lycoris cluster get nodes 2>/dev/null | grep -q "${node}.*active"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

echo "building static musl binary..."
cargo +stable build --release --target x86_64-unknown-linux-musl -p lycoris

echo "building container image..."
podman build -t "${IMAGE}" -f "${SCRIPT_DIR}/Dockerfile.shell-test" "${PROJECT_ROOT}" >/dev/null

echo "generating configs..."
mkdir -p "${E2E_DIR}/certs" "${E2E_DIR}/configs" "${E2E_DIR}/data" "${E2E_DIR}/keys"

for i in $(seq 0 $((NODE_COUNT - 1))); do
  mkdir -p "${E2E_DIR}/data/node-${i}"

  cat > "${E2E_DIR}/configs/node-${i}.toml" <<EOF
data_dir = "/data"

[node]
id = "node-${i}"
address = "https://node-${i}:5000"

[cluster]
listen_address = "0.0.0.0:5000"
bootstrap_peers = []

[tls]
ca_cert = "/certs/ca.crt"
ca_key = "/certs/ca.key"
cert = "/certs/node-${i}.crt"
key = "/certs/node-${i}.key"
EOF
done

echo "creating podman network..."
podman network create "${NETWORK}" >/dev/null

echo "starting bootstrap node (node-0)..."
# The daemon config is mounted at the default user config path, so both the
# daemon (default lookup) and the CLI subcommands that load it (join/leave)
# find it without extra wiring.
podman run -d --name "node-0" \
  --network "${NETWORK}" --hostname "node-0" \
  -v "${E2E_DIR}/certs:/certs" \
  -v "${E2E_DIR}/configs/node-0.toml:/root/.config/lycoris/lycoris.toml:ro" \
  -v "${E2E_DIR}/data/node-0:/data" \
  "${IMAGE}" >/dev/null

echo "waiting for node-0 to generate CA and certificates..."
if ! wait_for_file "${E2E_DIR}/certs/ca.crt" || ! wait_for_file "${E2E_DIR}/certs/ca.key"; then
  echo "error: node-0 failed to generate CA certificates" >&2
  podman logs node-0 >&2 || true
  exit 1
fi

echo "initializing cluster on node-0..."
run_in_node 0 lycoris cluster init

CLUSTER_KEY="$(run_in_node 0 lycoris cluster key)"
echo "cluster key: ${CLUSTER_KEY}"

echo "copying cluster key to shared keys directory..."
podman cp "node-0:/root/.local/share/lycoris/cluster.key" "${E2E_DIR}/keys/cluster.key"
echo "copying cluster key into node-0 data directory..."
podman cp "${E2E_DIR}/keys/cluster.key" "node-0:/data/cluster.key"

echo "restarting node-0 to pick up cluster key..."
podman restart node-0 >/dev/null

echo "waiting for node-0 to be ready after restart..."
if ! wait_until_ready 0; then
  echo "error: node-0 did not become ready after restart" >&2
  podman logs node-0 >&2 || true
  exit 1
fi

echo "copying shared CA to node-1 and node-2..."
for i in $(seq 1 $((NODE_COUNT - 1))); do
  mkdir -p "${E2E_DIR}/certs/node-${i}"
  cp "${E2E_DIR}/certs/ca.crt" "${E2E_DIR}/certs/node-${i}/ca.crt"
  cp "${E2E_DIR}/certs/ca.key" "${E2E_DIR}/certs/node-${i}/ca.key"

  sed -i "s|/certs/ca.crt|/certs/node-${i}/ca.crt|; s|/certs/ca.key|/certs/node-${i}/ca.key|" \
    "${E2E_DIR}/configs/node-${i}.toml"
done

echo "starting remaining nodes..."
for i in $(seq 1 $((NODE_COUNT - 1))); do
  podman run -d --name "node-${i}" \
    --network "${NETWORK}" --hostname "node-${i}" \
    -v "${E2E_DIR}/certs:/certs" \
    -v "${E2E_DIR}/configs/node-${i}.toml:/root/.config/lycoris/lycoris.toml:ro" \
    -v "${E2E_DIR}/data/node-${i}:/data" \
    -v "${E2E_DIR}/keys/cluster.key:/data/cluster.key:ro" \
    -v "${E2E_DIR}/keys/cluster.key:/root/.local/share/lycoris/cluster.key:ro" \
    "${IMAGE}" >/dev/null
done

echo "waiting for nodes to generate certificates..."
for i in $(seq 1 $((NODE_COUNT - 1))); do
  if ! wait_for_file "${E2E_DIR}/certs/node-${i}.crt"; then
    echo "error: node-${i} failed to generate certificate" >&2
    podman logs "node-${i}" >&2 || true
    exit 1
  fi
done

echo "joining node-1 and node-2 to the cluster..."
# No --key flag: join must pick up the key from the local cluster key file.
for i in $(seq 1 $((NODE_COUNT - 1))); do
  run_in_node "${i}" lycoris cluster join \
    --peer "https://node-0:5000"
done

echo "waiting for cluster to converge..."
if ! wait_until_nodes_visible 0; then
  echo "error: cluster did not converge in time" >&2
  exit 1
fi

echo ""
echo "=== verifying lycoris cluster key on node-1 ==="
NODE1_KEY="$(run_in_node 1 lycoris cluster key)"
if [[ "${NODE1_KEY}" != "${CLUSTER_KEY}" ]]; then
  echo "error: node-1 cluster key does not match node-0" >&2
  exit 1
fi
echo "ok: cluster key matches"

echo ""
echo "=== verifying lycoris cluster nodes on node-0 ==="
NODES_OUTPUT="$(run_in_node 0 lycoris cluster get nodes)"
echo "${NODES_OUTPUT}"
for i in $(seq 0 $((NODE_COUNT - 1))); do
  if ! echo "${NODES_OUTPUT}" | grep -q "node-${i}"; then
    echo "error: node-${i} not found in cluster nodes output" >&2
    exit 1
  fi
done
if ! echo "${NODES_OUTPUT}" | grep -q -- "->"; then
  echo "error: current node not highlighted" >&2
  exit 1
fi
echo "ok: all nodes present and current node highlighted"

echo ""
echo "=== verifying lycoris cluster get node on node-0 ==="
GET_OUTPUT="$(run_in_node 0 lycoris cluster get node node-1)"
echo "${GET_OUTPUT}"
if ! echo "${GET_OUTPUT}" | grep -q "address:"; then
  echo "error: get output missing address field" >&2
  exit 1
fi
if ! echo "${GET_OUTPUT}" | grep -q "state:"; then
  echo "error: get output missing state field" >&2
  exit 1
fi
if ! echo "${GET_OUTPUT}" | grep -q "incarnation:"; then
  echo "error: get output missing incarnation field" >&2
  exit 1
fi
if ! echo "${GET_OUTPUT}" | grep -q "heartbeat:"; then
  echo "error: get output missing heartbeat field" >&2
  exit 1
fi
echo "ok: get output looks correct"

echo ""
echo "=== verifying resource smoke tests on node-0 ==="
SKILLS_OUTPUT="$(run_in_node 0 lycoris cluster get skills)"
echo "${SKILLS_OUTPUT}"
if ! echo "${SKILLS_OUTPUT}" | grep -q "total:"; then
  echo "error: skills output missing total" >&2
  exit 1
fi
SESSIONS_OUTPUT="$(run_in_node 0 lycoris cluster get sessions)"
echo "${SESSIONS_OUTPUT}"
if ! echo "${SESSIONS_OUTPUT}" | grep -q "total:"; then
  echo "error: sessions output missing total" >&2
  exit 1
fi
echo "ok: resource smoke tests passed"

echo ""
echo "=== verifying lycoris cluster leave on node-2 ==="
run_in_node 2 lycoris cluster leave

echo "waiting for leave state to propagate..."
if ! wait_until_not_active 0 node-2; then
  echo "error: node-2 is still active after leave (timed out waiting for propagation)" >&2
  exit 1
fi

echo ""
echo "=== verifying node-2 is no longer active ==="
NODES_OUTPUT_AFTER="$(run_in_node 0 lycoris cluster get nodes)"
echo "${NODES_OUTPUT_AFTER}"
if echo "${NODES_OUTPUT_AFTER}" | grep -q "node-2.*active"; then
  echo "error: node-2 is still active after leave" >&2
  exit 1
fi
echo "ok: node-2 is not active after leave"

echo ""
echo "shell e2e passed"
