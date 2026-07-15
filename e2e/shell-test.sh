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
  local attempts=30
  while [[ ! -f "${path}" && ${attempts} -gt 0 ]]; do
    sleep 0.2
    attempts=$((attempts - 1))
  done
  [[ -f "${path}" ]]
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
podman run -d --name "node-0" \
  --network "${NETWORK}" --hostname "node-0" \
  -v "${E2E_DIR}/certs:/certs" \
  -v "${E2E_DIR}/configs/node-0.toml:/etc/lycoris/lycoris.toml:ro" \
  -v "${E2E_DIR}/data/node-0:/data" \
  "${IMAGE}" >/dev/null

# Make the daemon config discoverable by the CLI via the default user path.
run_in_node 0 mkdir -p /root/.config/lycoris
run_in_node 0 ln -sf /etc/lycoris/lycoris.toml /root/.config/lycoris/lycoris.toml

echo "waiting for node-0 to generate CA and certificates..."
sleep 3
if [[ ! -f "${E2E_DIR}/certs/ca.crt" || ! -f "${E2E_DIR}/certs/ca.key" ]]; then
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

echo "restarting node-0 to pick up cluster key..."
podman restart node-0 >/dev/null
sleep 2

echo "waiting for node-0 to be ready after restart..."
sleep 3

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
    -v "${E2E_DIR}/configs/node-${i}.toml:/etc/lycoris/lycoris.toml:ro" \
    -v "${E2E_DIR}/data/node-${i}:/data" \
    -v "${E2E_DIR}/keys:/root/.local/share/lycoris:ro" \
    "${IMAGE}" >/dev/null

  run_in_node "${i}" mkdir -p /root/.config/lycoris
  run_in_node "${i}" ln -sf /etc/lycoris/lycoris.toml /root/.config/lycoris/lycoris.toml
done

echo "waiting for nodes to generate certificates..."
sleep 3
for i in $(seq 1 $((NODE_COUNT - 1))); do
  if [[ ! -f "${E2E_DIR}/certs/node-${i}.crt" ]]; then
    echo "error: node-${i} failed to generate certificate" >&2
    podman logs "node-${i}" >&2 || true
    exit 1
  fi
done

echo "joining node-1 and node-2 to the cluster..."
for i in $(seq 1 $((NODE_COUNT - 1))); do
  run_in_node "${i}" lycoris cluster join \
    --peer "https://node-0:5000" \
    --key "${CLUSTER_KEY}"
done

echo "waiting for cluster to converge..."
sleep 6

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
echo "=== verifying lycoris cluster describe on node-0 ==="
DESCRIBE_OUTPUT="$(run_in_node 0 lycoris cluster describe node node-1)"
echo "${DESCRIBE_OUTPUT}"
if ! echo "${DESCRIBE_OUTPUT}" | grep -q "address:"; then
  echo "error: describe output missing address field" >&2
  exit 1
fi
if ! echo "${DESCRIBE_OUTPUT}" | grep -q "in-degree:"; then
  echo "error: describe output missing in-degree field" >&2
  exit 1
fi
if ! echo "${DESCRIBE_OUTPUT}" | grep -q "out-degree:"; then
  echo "error: describe output missing out-degree field" >&2
  exit 1
fi
echo "ok: describe output looks correct"

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
sleep 6

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
