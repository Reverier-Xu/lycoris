#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
E2E_DIR="$(mktemp -d)"
NETWORK="lycoris-e2e"
IMAGE="lycoris-e2e:latest"
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

echo "building static musl binary..."
cargo +stable build --release --target x86_64-unknown-linux-musl -p lycoris

echo "building container image..."
podman build -t "${IMAGE}" -f "${SCRIPT_DIR}/Containerfile" "${PROJECT_ROOT}"

echo "generating configs..."
mkdir -p "${E2E_DIR}/certs" "${E2E_DIR}/configs" "${E2E_DIR}/data"

for i in $(seq 0 $((NODE_COUNT - 1))); do
  peers=()
  for j in $(seq 0 $((NODE_COUNT - 1))); do
    if [ "${i}" -ne "${j}" ]; then
      peers+=("\"https://node-${j}:5000\"")
    fi
  done
  peers_list=$(IFS=,; echo "${peers[*]}")
  mkdir -p "${E2E_DIR}/data/node-${i}"

  cat > "${E2E_DIR}/configs/node-${i}.toml" <<EOF
data_dir = "/data"

[node]
id = "node-${i}"
address = "node-${i}:5000"

[cluster]
listen_address = "0.0.0.0:5000"
bootstrap_peers = [${peers_list}]

[tls]
ca_cert = "/certs/ca.crt"
ca_key = "/certs/ca.key"
cert = "/certs/node-${i}.crt"
key = "/certs/node-${i}.key"
EOF
done

echo "creating podman network..."
podman network create "${NETWORK}" >/dev/null

echo "starting bootstrap node (node-0) to generate CA..."
podman run -d --name "node-0" \
  --network "${NETWORK}" --hostname "node-0" \
  -v "${E2E_DIR}/certs:/certs" \
  -v "${E2E_DIR}/configs/node-0.toml:/config.toml:ro" \
  -v "${E2E_DIR}/data/node-0:/data" \
  "${IMAGE}" >/dev/null

# Wait for node-0 to generate the CA and its own certificate.
sleep 3

if [ ! -f "${E2E_DIR}/certs/ca.crt" ] || [ ! -f "${E2E_DIR}/certs/ca.key" ]; then
  echo "node-0 failed to generate CA certificates" >&2
  podman logs node-0 >&2 || true
  exit 1
fi

echo "copying shared CA to remaining nodes..."
for i in $(seq 1 $((NODE_COUNT - 1))); do
  # The daemon expects ca.crt / ca.key in its own certs directory, so place
  # the shared CA there before starting the node.
  mkdir -p "${E2E_DIR}/certs/node-${i}"
  cp "${E2E_DIR}/certs/ca.crt" "${E2E_DIR}/certs/node-${i}/ca.crt"
  cp "${E2E_DIR}/certs/ca.key" "${E2E_DIR}/certs/node-${i}/ca.key"

  # Update the config to point to the per-node cert directory.
  sed -i "s|/certs/ca.crt|/certs/node-${i}/ca.crt|; s|/certs/ca.key|/certs/node-${i}/ca.key|" \
    "${E2E_DIR}/configs/node-${i}.toml"
done

echo "starting remaining nodes..."
for i in $(seq 1 $((NODE_COUNT - 1))); do
  podman run -d --name "node-${i}" \
    --network "${NETWORK}" --hostname "node-${i}" \
    -v "${E2E_DIR}/certs:/certs" \
    -v "${E2E_DIR}/configs/node-${i}.toml:/config.toml:ro" \
    -v "${E2E_DIR}/data/node-${i}:/data" \
    "${IMAGE}" >/dev/null
done

echo "waiting for cluster to settle..."
sleep 4

echo "running e2e client..."
cargo +stable build --target x86_64-unknown-linux-musl --example cluster_client -p lycoris-daemon
CLIENT_BIN="${PROJECT_ROOT}/target/x86_64-unknown-linux-musl/debug/examples/cluster_client"

podman run --rm --network "${NETWORK}" \
  -v "${CLIENT_BIN}:/client:ro" \
  -v "${E2E_DIR}/certs:/certs:ro" \
  --tmpfs /tmp \
  --entrypoint /client \
  "${IMAGE}" \
  "https://node-0:5000" "https://node-2:5000" \
  "/certs/ca.crt" "/certs/node-0.crt" "/certs/node-0.key" \
  "e2e-external-node"

echo "e2e passed"
