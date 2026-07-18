#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
E2E_DIR="$(mktemp -d)"
NETWORK_MAIN="lycoris-partition-main"
NETWORK_A="lycoris-partition-a"
IMAGE="lycoris-shell-test:latest"
NODE_COUNT=6

cleanup() {
  echo "cleaning up..."
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    podman rm -f "node-${i}" >/dev/null 2>&1 || true
  done
  podman network rm -f "${NETWORK_MAIN}" >/dev/null 2>&1 || true
  podman network rm -f "${NETWORK_A}" >/dev/null 2>&1 || true
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

# Poll until the observer reports every node as active.
wait_until_all_active() {
  local observer="$1"
  local timeout_secs="${2:-60}"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    local output converged=true
    output="$(run_in_node "${observer}" lycoris cluster get nodes 2>/dev/null || true)"
    for i in $(seq 0 $((NODE_COUNT - 1))); do
      if ! echo "${output}" | grep -q "node-${i}.*active"; then
        converged=false
        break
      fi
    done
    ${converged} && return 0
    sleep 1
  done
  return 1
}

network_subnet() {
  local network="$1"
  podman network inspect "${network}" --format '{{range .Subnets}}{{.Subnet}} {{end}}' | awk '{print $1}'
}

apply_partition() {
  local main_subnet="$1"
  local a_subnet="$2"

  echo "blocking cross-segment traffic inside containers..."
  for i in 0 1 2; do
    run_in_node "${i}" iptables -A INPUT -s "${main_subnet}" -j DROP
    run_in_node "${i}" iptables -A OUTPUT -d "${main_subnet}" -j DROP
  done
  for i in 3 4 5; do
    run_in_node "${i}" iptables -A INPUT -s "${a_subnet}" -j DROP
    run_in_node "${i}" iptables -A OUTPUT -d "${a_subnet}" -j DROP
  done
}

clear_partition() {
  echo "removing container firewall rules..."
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    run_in_node "${i}" iptables -F >/dev/null 2>&1 || true
  done
}

echo "building static musl binary..."
cargo +stable build --release --target x86_64-unknown-linux-musl -p lycoris

echo "building container image..."
podman build -t "${IMAGE}" -f "${SCRIPT_DIR}/Dockerfile.shell-test" "${PROJECT_ROOT}" >/dev/null

echo "verifying iptables works inside the test container..."
if ! podman run --rm --cap-add=net_admin --entrypoint iptables "${IMAGE}" -L -n >/dev/null 2>&1; then
  echo "error: iptables is not functional inside the test container" >&2
  exit 1
fi

echo "generating configs..."
mkdir -p "${E2E_DIR}/certs" "${E2E_DIR}/configs" "${E2E_DIR}/data" "${E2E_DIR}/keys"

for i in $(seq 0 $((NODE_COUNT - 1))); do
  mkdir -p "${E2E_DIR}/data/node-${i}"

  peers=""
  if [[ $i -lt $((NODE_COUNT - 1)) ]]; then
    peers="\"https://node-$((i + 1)):5000\""
  fi
  if [[ $i -gt 0 ]]; then
    if [[ -n "${peers}" ]]; then
      peers="${peers}, "
    fi
    peers="${peers}\"https://node-$((i - 1)):5000\""
  fi

  cat > "${E2E_DIR}/configs/node-${i}.toml" <<EOF
data_dir = "/data"

[node]
id = "node-${i}"
address = "https://node-${i}:5000"

[cluster]
listen_address = "0.0.0.0:5000"
bootstrap_peers = [${peers}]

[tls]
ca_cert = "/certs/ca.crt"
ca_key = "/certs/ca.key"
cert = "/certs/node-${i}.crt"
key = "/certs/node-${i}.key"
EOF
done

echo "creating podman networks..."
podman network create "${NETWORK_MAIN}" >/dev/null
podman network create "${NETWORK_A}" >/dev/null

MAIN_SUBNET="$(network_subnet "${NETWORK_MAIN}")"
A_SUBNET="$(network_subnet "${NETWORK_A}")"
echo "main subnet: ${MAIN_SUBNET}, partition subnet: ${A_SUBNET}"

echo "starting bootstrap node (node-0)..."
# The daemon config is mounted at the default user config path, so both the
# daemon (default lookup) and the CLI subcommands that load it (join/leave)
# find it without extra wiring.
podman run -d --name "node-0" --cap-add=net_admin \
  --network "${NETWORK_MAIN}" --hostname "node-0" \
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

echo "copying shared CA to remaining nodes..."
for i in $(seq 1 $((NODE_COUNT - 1))); do
  mkdir -p "${E2E_DIR}/certs/node-${i}"
  cp "${E2E_DIR}/certs/ca.crt" "${E2E_DIR}/certs/node-${i}/ca.crt"
  cp "${E2E_DIR}/certs/ca.key" "${E2E_DIR}/certs/node-${i}/ca.key"

  sed -i "s|/certs/ca.crt|/certs/node-${i}/ca.crt|; s|/certs/ca.key|/certs/node-${i}/ca.key|" \
    "${E2E_DIR}/configs/node-${i}.toml"
done

echo "starting remaining nodes..."
for i in $(seq 1 $((NODE_COUNT - 1))); do
  podman run -d --name "node-${i}" --cap-add=net_admin \
    --network "${NETWORK_MAIN}" --hostname "node-${i}" \
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

echo "joining nodes to form a line topology..."
# No --key flag: join must pick up the key from the local cluster key file.
for i in $(seq 1 $((NODE_COUNT - 1))); do
  peer="https://node-$((i - 1)):5000"
  run_in_node "${i}" lycoris cluster join \
    --peer "${peer}"
done

echo "waiting for cluster to converge..."
if ! wait_until_all_active 0; then
  echo "error: cluster did not converge in time" >&2
  exit 1
fi

echo ""
echo "=== baseline: all 6 nodes visible on node-0 ==="
BASELINE="$(run_in_node 0 lycoris cluster get nodes)"
echo "${BASELINE}"
for i in $(seq 0 $((NODE_COUNT - 1))); do
  if ! echo "${BASELINE}" | grep -q "node-${i}.*active"; then
    echo "error: node-${i} not active at baseline" >&2
    exit 1
  fi
done
echo "ok: baseline convergence"

echo ""
echo "=== scenario 1: partition [node-0,1,2] from [node-3,4,5] ==="
echo "moving left segment to isolated network..."
for i in 0 1 2; do
  podman network disconnect "${NETWORK_MAIN}" "node-${i}" >/dev/null
  podman network connect "${NETWORK_A}" "node-${i}" >/dev/null
done

apply_partition "${MAIN_SUBNET}" "${A_SUBNET}"

echo "waiting for partition to settle..."
settle_deadline=$((SECONDS + 45))
while (( SECONDS < settle_deadline )); do
  LEFT="$(run_in_node 0 lycoris cluster get nodes 2>/dev/null || true)"
  RIGHT="$(run_in_node 5 lycoris cluster get nodes 2>/dev/null || true)"

  left_ok=true
  for i in 3 4 5; do
    if echo "${LEFT}" | grep -q "node-${i}.*active"; then
      left_ok=false
      break
    fi
  done

  right_ok=true
  for i in 0 1 2; do
    if echo "${RIGHT}" | grep -q "node-${i}.*active"; then
      right_ok=false
      break
    fi
  done

  if ${left_ok} && ${right_ok}; then
    break
  fi
  sleep 1
done

echo "querying left segment (node-0)..."
LEFT="$(run_in_node 0 lycoris cluster get nodes)"
echo "${LEFT}"
for i in 0 1 2; do
  if ! echo "${LEFT}" | grep -q "node-${i}.*active"; then
    echo "error: node-${i} not visible/active on left segment" >&2
    exit 1
  fi
done
for i in 3 4 5; do
  if echo "${LEFT}" | grep -q "node-${i}.*active"; then
    echo "error: right-side node-${i} should not be active on left segment" >&2
    exit 1
  fi
done
echo "ok: left segment is isolated and consistent"

echo ""
echo "querying right segment (node-5)..."
RIGHT="$(run_in_node 5 lycoris cluster get nodes)"
echo "${RIGHT}"
for i in 3 4 5; do
  if ! echo "${RIGHT}" | grep -q "node-${i}.*active"; then
    echo "error: node-${i} not visible/active on right segment" >&2
    exit 1
  fi
done
for i in 0 1 2; do
  if echo "${RIGHT}" | grep -q "node-${i}.*active"; then
    echo "error: left-side node-${i} should not be active on right segment" >&2
    exit 1
  fi
done
echo "ok: right segment is isolated and consistent"

echo ""
echo "=== scenario 1: recovering network connection ==="
echo "reconnecting left segment to main network..."
for i in 0 1 2; do
  podman network disconnect "${NETWORK_A}" "node-${i}" >/dev/null
  podman network connect "${NETWORK_MAIN}" "node-${i}" >/dev/null
done
clear_partition

echo "waiting for cluster to heal after recovery..."
if ! wait_until_all_active 0; then
  echo "error: cluster did not heal after recovery" >&2
  exit 1
fi

echo "querying node-0 after recovery..."
RECOVERED="$(run_in_node 0 lycoris cluster get nodes)"
echo "${RECOVERED}"
for i in $(seq 0 $((NODE_COUNT - 1))); do
  if ! echo "${RECOVERED}" | grep -q "node-${i}.*active"; then
    echo "error: node-${i} not active after recovery" >&2
    exit 1
  fi
done
echo "ok: partition healed, all nodes active"

echo ""
echo "=== scenario 2: middle node (node-2) leaves the cluster ==="
run_in_node 2 lycoris cluster leave
echo "stopping node-2 container to complete departure..."
podman stop -t 0 node-2 >/dev/null

echo "waiting for leave to propagate and topology to repair..."
repair_deadline=$((SECONDS + 60))
repaired=false
while (( SECONDS < repair_deadline )); do
  NODE0_VIEW="$(run_in_node 0 lycoris cluster get nodes 2>/dev/null || true)"
  NODE1_VIEW="$(run_in_node 1 lycoris cluster get nodes 2>/dev/null || true)"
  if ! echo "${NODE0_VIEW}" | grep -q "node-2.*active" \
    && echo "${NODE1_VIEW}" | grep -q "node-3.*active"; then
    repaired=true
    break
  fi
  sleep 1
done
if ! ${repaired}; then
  echo "error: cluster did not heal after node-2 left" >&2
  exit 1
fi

echo "querying node-0 after node-2 left..."
AFTER_LEAVE="$(run_in_node 0 lycoris cluster get nodes)"
echo "${AFTER_LEAVE}"

if echo "${AFTER_LEAVE}" | grep -q "node-2.*active"; then
  echo "error: node-2 is still active after leave" >&2
  exit 1
fi
for i in 0 1 3 4 5; do
  if ! echo "${AFTER_LEAVE}" | grep -q "node-${i}.*active"; then
    echo "error: node-${i} not active after node-2 left" >&2
    exit 1
  fi
done
echo "ok: node-2 left, remaining nodes stay active"

echo ""
echo "=== verifying left and right sides reconnected through fallback ==="
echo "querying node-1 (left of departed node-2)..."
NODE1_VIEW="$(run_in_node 1 lycoris cluster get nodes)"
echo "${NODE1_VIEW}"
if ! echo "${NODE1_VIEW}" | grep -q "node-3.*active"; then
  echo "error: node-1 cannot see node-3 after node-2 left" >&2
  exit 1
fi

echo "querying node-3 (right of departed node-2)..."
NODE3_VIEW="$(run_in_node 3 lycoris cluster get nodes)"
echo "${NODE3_VIEW}"
if ! echo "${NODE3_VIEW}" | grep -q "node-1.*active"; then
  echo "error: node-3 cannot see node-1 after node-2 left" >&2
  exit 1
fi

echo "ok: node-1 and node-3 see each other after the relay node left"

echo ""
echo "partition e2e passed"
