#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
NETWORK_LEFT="lycoris-e2e-left"
NETWORK_RIGHT="lycoris-e2e-right"
WAIT_MS=10000

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker compose"
  EXEC="docker"
elif command -v docker-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker-compose"
  if command -v docker >/dev/null 2>&1; then
    EXEC="docker"
  else
    EXEC="podman"
    if [[ -z "${DOCKER_HOST:-}" && -S "/run/user/$(id -u)/podman/podman.sock" ]]; then
      export DOCKER_HOST="unix:///run/user/$(id -u)/podman/podman.sock"
    fi
  fi
elif command -v podman >/dev/null 2>&1 && command -v podman-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE="podman-compose"
  EXEC="podman"
else
  echo "error: neither docker compose nor podman-compose found" >&2
  exit 1
fi

for command in jq openssl timeout; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "error: required command not found: ${command}" >&2
    exit 1
  fi
done

cd "${SCRIPT_DIR}"

cleanup() {
  echo "=== cleaning up ==="
  ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" down -v || true
}
trap cleanup EXIT

strip_ansi() {
  sed 's/\x1b\[[0-9;]*m//g'
}

now_ms() {
  date +%s%3N
}

remaining_ms() {
  local deadline="$1"
  local remaining=$((deadline - $(now_ms)))
  if (( remaining > 0 )); then
    printf '%s\n' "${remaining}"
  else
    printf '0\n'
  fi
}

sleep_until_next_poll() {
  local deadline="$1"
  local remaining
  remaining="$(remaining_ms "${deadline}")"
  if (( remaining <= 0 )); then
    return
  fi
  local delay_ms=200
  if (( remaining < delay_ms )); then
    delay_ms="${remaining}"
  fi
  sleep "0.$(printf '%03d' "${delay_ms}")"
}

cluster_output() {
  local budget_ms="$1"
  local container="$2"
  shift 2
  local timeout_seconds
  printf -v timeout_seconds '%d.%03ds' "$((budget_ms / 1000))" "$((budget_ms % 1000))"
  timeout "${timeout_seconds}" \
    "${EXEC}" exec -i "${container}" lycoris cluster "$@" 2>&1 | strip_ansi
}

query_with_deadline() {
  local deadline="$1"
  local container="$2"
  shift 2
  local budget
  budget="$(remaining_ms "${deadline}")"
  if (( budget <= 0 )); then
    return 124
  fi
  if (( budget > 1000 )); then
    budget=1000
  fi
  cluster_output "${budget}" "${container}" "$@"
}

diagnostics() {
  ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" ps >&2 || true
  for container in lycoris-e2e-node-0 lycoris-e2e-node-1 lycoris-e2e-node-2; do
    echo "--- ${container} logs ---" >&2
    "${EXEC}" logs --tail 100 "${container}" >&2 || true
  done
}

wait_for_control() {
  local container="$1"
  local deadline=$(($(now_ms) + WAIT_MS))
  while (( $(remaining_ms "${deadline}") > 0 )); do
    if query_with_deadline "${deadline}" "${container}" get nodes >/dev/null; then
      return 0
    fi
    sleep_until_next_poll "${deadline}"
  done
  echo "error: ${container} control plane was not ready within 10 seconds" >&2
  diagnostics
  return 1
}

output_has_active_node() {
  local output="$1"
  local node_id="${2,,}"
  while IFS= read -r line; do
    local normalized="${line,,}"
    if [[ "${normalized}" == *"${node_id}"* && "${normalized}" == *$'\tactive'* ]]; then
      return 0
    fi
  done <<< "${output}"
  return 1
}

wait_for_active_node() {
  local container="$1"
  local node_id="$2"
  local deadline=$(($(now_ms) + WAIT_MS))
  while (( $(remaining_ms "${deadline}") > 0 )); do
    local output=""
    output="$(query_with_deadline "${deadline}" "${container}" get nodes || true)"
    if output_has_active_node "${output}" "${node_id}"; then
      return 0
    fi
    sleep_until_next_poll "${deadline}"
  done
  echo "error: ${container} did not observe ${node_id} active within 10 seconds" >&2
  diagnostics
  return 1
}

wait_for_extension() {
  local container="$1"
  local extension_id="$2"
  local deadline=$(($(now_ms) + WAIT_MS))
  while (( $(remaining_ms "${deadline}") > 0 )); do
    local output=""
    output="$(query_with_deadline "${deadline}" "${container}" get extensions || true)"
    if [[ "${output}" == *"${extension_id}"* ]]; then
      return 0
    fi
    sleep_until_next_poll "${deadline}"
  done
  echo "error: ${container} did not observe ${extension_id} within 10 seconds" >&2
  diagnostics
  return 1
}

wait_for_echo_route() {
  local deadline=$(($(now_ms) + WAIT_MS))
  while (( $(remaining_ms "${deadline}") > 0 )); do
    local output=""
    output="$(query_with_deadline \
      "${deadline}" lycoris-e2e-node-0 ext invoke echo-ext echo '{"ok":true}' || true)"
    if [[ "${output}" == *'"method":"echo"'* && "${output}" == *'"ok":true'* ]]; then
      return 0
    fi
    sleep_until_next_poll "${deadline}"
  done
  echo "error: echo extension route did not converge within 10 seconds" >&2
  diagnostics
  return 1
}

identity_json() {
  local container="$1"
  cluster_output 2000 "${container}" identity --json
}

initialize_node() {
  local service="$1"
  shift
  timeout 10s ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" run --rm --no-deps \
    --entrypoint lycoris "${service}" cluster "$@"
}

capture_identity() {
  local container="$1"
  local json
  json="$(identity_json "${container}")"
  local node_id peer_id
  node_id="$(printf '%s' "${json}" | jq -er '.node_id')"
  peer_id="$(printf '%s' "${json}" | jq -er '.peer_id')"
  if [[ -z "${node_id}" || -z "${peer_id}" ]]; then
    echo "error: incomplete identity from ${container}: ${json}" >&2
    return 1
  fi
  printf '%s\t%s\n' "${node_id}" "${peer_id}"
}

echo "=== resetting e2e state ==="
${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" down -v || true
./gen-certs.sh
./gen-configs.sh
CLUSTER_KEY="$(openssl rand -hex 32)"

echo "=== building static musl binary and container image ==="
cd "${PROJECT_DIR}"
cargo build --release --target x86_64-unknown-linux-musl -p lycoris --locked
cd "${SCRIPT_DIR}"
${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" build node-0

echo "=== initializing sponsor node-0 ==="
initialize_node node-0 init --key "${CLUSTER_KEY}"
${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" up -d node-0
wait_for_control lycoris-e2e-node-0
IFS=$'\t' read -r NODE0_ID NODE0_PEER < <(capture_identity lycoris-e2e-node-0)
NODE0_SPONSOR="/dns4/node-0/tcp/5001/p2p/${NODE0_PEER}"

echo "=== admitting node-1 through node-0 ==="
initialize_node node-1 join --peer "${NODE0_SPONSOR}" --key "${CLUSTER_KEY}"
${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" up -d node-1
wait_for_control lycoris-e2e-node-1
IFS=$'\t' read -r NODE1_ID NODE1_PEER < <(capture_identity lycoris-e2e-node-1)
wait_for_active_node lycoris-e2e-node-0 "${NODE1_ID}"
wait_for_active_node lycoris-e2e-node-1 "${NODE0_ID}"

# Restart once after admission so the persisted sponsor address enters the
# configured reconnect directory used after a transport loss.
"${EXEC}" restart lycoris-e2e-node-1 >/dev/null
wait_for_control lycoris-e2e-node-1
IFS=$'\t' read -r NODE1_RESTARTED_ID NODE1_RESTARTED_PEER < <(capture_identity lycoris-e2e-node-1)
[[ "${NODE1_RESTARTED_ID}" == "${NODE1_ID}" ]]
[[ "${NODE1_RESTARTED_PEER}" == "${NODE1_PEER}" ]]

NODE1_SPONSOR="/dns4/node-1/tcp/5001/p2p/${NODE1_PEER}"
echo "=== admitting node-2 through node-1 ==="
initialize_node node-2 join --peer "${NODE1_SPONSOR}" --key "${CLUSTER_KEY}"
${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" up -d node-2
wait_for_control lycoris-e2e-node-2
IFS=$'\t' read -r NODE2_ID NODE2_PEER < <(capture_identity lycoris-e2e-node-2)
wait_for_active_node lycoris-e2e-node-0 "${NODE2_ID}"
wait_for_active_node lycoris-e2e-node-2 "${NODE0_ID}"

"${EXEC}" restart lycoris-e2e-node-2 >/dev/null
wait_for_control lycoris-e2e-node-2
IFS=$'\t' read -r NODE2_RESTARTED_ID NODE2_RESTARTED_PEER < <(capture_identity lycoris-e2e-node-2)
[[ "${NODE2_RESTARTED_ID}" == "${NODE2_ID}" ]]
[[ "${NODE2_RESTARTED_PEER}" == "${NODE2_PEER}" ]]

echo "=== partitioning the sparse overlay ==="
"${EXEC}" network disconnect "${NETWORK_LEFT}" lycoris-e2e-node-1
"${EXEC}" network disconnect "${NETWORK_RIGHT}" lycoris-e2e-node-1

cluster_output 10000 lycoris-e2e-node-0 ext load /fixtures/echo.pkg.toml
# Span at least one five-second resource-sync cadence while the bridge has no
# network, then require a successful local query before asserting absence.
sleep 6
if ! output="$(cluster_output 1000 lycoris-e2e-node-2 get extensions)"; then
  echo "error: node-2 extension query failed during the partition" >&2
  diagnostics
  exit 1
fi
if [[ "${output}" == *"echo-ext"* ]]; then
  echo "error: echo-ext crossed a disconnected partition" >&2
  diagnostics
  exit 1
fi

echo "=== reconnecting and verifying recovery ==="
"${EXEC}" network connect --alias node-1 "${NETWORK_LEFT}" lycoris-e2e-node-1
"${EXEC}" network connect --alias node-1 "${NETWORK_RIGHT}" lycoris-e2e-node-1
wait_for_active_node lycoris-e2e-node-0 "${NODE2_ID}"
wait_for_active_node lycoris-e2e-node-2 "${NODE0_ID}"
wait_for_extension lycoris-e2e-node-2 echo-ext
wait_for_echo_route

echo "=== docker overlay e2e passed ==="
echo "node-0: ${NODE0_ID}"
echo "node-1: ${NODE1_ID}"
echo "node-2: ${NODE2_ID}"
