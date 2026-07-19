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
  podman rm -f openai-mock >/dev/null 2>&1 || true
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

# Poll until an extension id shows up in the observer's extension listing.
wait_until_extension_visible() {
  local observer="$1"
  local extension="$2"
  local timeout_secs="${3:-60}"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    if run_in_node "${observer}" lycoris cluster get extensions 2>/dev/null | grep -q "${extension}"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Poll until the observer sees the capability annotation `ext.<id>` on the
# serving node's register (rendered by the single-node get output).
wait_until_extension_announced() {
  local observer="$1"
  local node="$2"
  local extension="$3"
  local timeout_secs="${4:-60}"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    if run_in_node "${observer}" lycoris cluster get node "${node}" 2>/dev/null | grep -q "ext.${extension}"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

echo "building static musl binary..."
build_start=${SECONDS}
cargo +stable build --release --target x86_64-unknown-linux-musl -p lycoris
echo "musl binary built in $((SECONDS - build_start))s"

echo "building the wasm llm provider extension..."
if ! rustup target list --installed | grep -q wasm32-unknown-unknown; then
  rustup target add wasm32-unknown-unknown
fi
build_start=${SECONDS}
cargo build --locked --release --target wasm32-unknown-unknown -p lycoris-ext-openai
echo "wasm extension built in $((SECONDS - build_start))s"

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

# node-1 carries the label the echo extension's selector matches; the runtime
# merges it into the node-local store at startup.
cat >> "${E2E_DIR}/configs/node-1.toml" <<EOF

[node.labels]
role = "runner"
EOF

# node-1 also carries the node-local settings of the openai wasm provider:
# the api key and base url never leave the node (llm-provider design,
# section 5). The base url points at the canned mock on the podman network.
cat >> "${E2E_DIR}/configs/node-1.toml" <<EOF

[extensions.local.openai]
api_key = "sk-e2e"
base_url = "http://openai-mock/v1"
EOF

echo "creating podman network..."
podman network create "${NETWORK}" >/dev/null

echo "starting the canned openai api (no real key involved)..."
podman run -d --name "openai-mock" \
  --network "${NETWORK}" --hostname "openai-mock" \
  -v "${SCRIPT_DIR}/openai-mock.conf:/etc/nginx/conf.d/default.conf:ro" \
  docker.io/library/nginx:alpine >/dev/null

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
echo "=== verifying extension registration and routing ==="
echo "copying extension fixtures into node-0..."
podman cp "${SCRIPT_DIR}/fixtures" "node-0:/fixtures"

echo "registering the echo extension from node-0..."
LOAD_OUTPUT="$(run_in_node 0 lycoris cluster ext load /fixtures/echo.pkg.toml)"
echo "${LOAD_OUTPUT}"
# The CLI colorizes values, so assert the label and the value separately.
if ! echo "${LOAD_OUTPUT}" | grep -qE "accepted:.*true"; then
  echo "error: extension registration was not accepted" >&2
  exit 1
fi
if ! echo "${LOAD_OUTPUT}" | grep -q "content hash:"; then
  echo "error: extension registration output missing content hash" >&2
  exit 1
fi
echo "ok: extension registered"

echo "waiting for the extension to converge to node-1..."
if ! wait_until_extension_visible 1 "echo-ext"; then
  echo "error: echo-ext did not become visible on node-1" >&2
  run_in_node 1 lycoris cluster get extensions >&2 || true
  exit 1
fi

echo "waiting for node-1 to announce the ext.echo-ext capability..."
if ! wait_until_extension_announced 0 "node-1" "echo-ext"; then
  echo "error: node-1 did not announce ext.echo-ext in time" >&2
  run_in_node 0 lycoris cluster get node node-1 >&2 || true
  exit 1
fi
echo "ok: node-1 runs the extension and advertises it"

# The announcing register carries node-1's configured label too: the
# `[node] labels` config surface reached membership end to end.
NODE1_DETAIL="$(run_in_node 0 lycoris cluster get node node-1)"
echo "${NODE1_DETAIL}"
if ! echo "${NODE1_DETAIL}" | grep -qE '"role": "runner"'; then
  echo "error: node-1 register does not carry the configured role=runner label" >&2
  exit 1
fi
echo "ok: node-1 register carries the configured label"

NODE0_DETAIL="$(run_in_node 0 lycoris cluster get node node-0)"
if echo "${NODE0_DETAIL}" | grep -q "ext.echo-ext"; then
  echo "error: node-0 must not advertise echo-ext (its labels do not match the selector)" >&2
  echo "${NODE0_DETAIL}" >&2
  exit 1
fi
echo "ok: node-0 does not advertise the extension (selector mismatch)"

echo "invoking echo-ext from node-0..."
INVOKE_OUTPUT="$(run_in_node 0 lycoris cluster ext invoke echo-ext echo '{"k":"v"}' 2>&1)"
echo "${INVOKE_OUTPUT}"
if ! echo "${INVOKE_OUTPUT}" | grep -q "executed by: node-1"; then
  echo "error: invocation was not routed to node-1" >&2
  exit 1
fi
if ! echo "${INVOKE_OUTPUT}" | grep -q '"k":"v"'; then
  echo "error: invocation payload was not echoed back" >&2
  exit 1
fi
echo "ok: invocation routed to node-1 and payload echoed"

echo ""
echo "=== verifying extension detail and listing on node-0 ==="
EXT_DETAIL="$(run_in_node 0 lycoris cluster get ext echo-ext)"
echo "${EXT_DETAIL}"
for field in "engine:" "content hash:" "artifact size:" "manifest:"; do
  if ! echo "${EXT_DETAIL}" | grep -q "${field}"; then
    echo "error: extension detail missing ${field}" >&2
    exit 1
  fi
done
if echo "${EXT_DETAIL}" | grep -q "function invoke"; then
  echo "error: extension detail must not dump the artifact body" >&2
  exit 1
fi
EXT_LIST="$(run_in_node 0 lycoris cluster get ext)"
echo "${EXT_LIST}"
if ! echo "${EXT_LIST}" | grep -q "echo-ext"; then
  echo "error: extension listing missing echo-ext" >&2
  exit 1
fi
echo "ok: extension detail and listing look correct"

echo ""
echo "=== verifying the wasm llm provider scenario ==="
echo "staging the openai package with the built wasm artifact..."
# The committed fixture names the artifact relative to the package file, so
# stage the release build next to it before copying both into node-0.
mkdir -p "${E2E_DIR}/openai-fixture"
cp "${SCRIPT_DIR}/fixtures/openai.pkg.toml" "${E2E_DIR}/openai-fixture/"
cp "${PROJECT_ROOT}/target/wasm32-unknown-unknown/release/lycoris_ext_openai.wasm" \
  "${E2E_DIR}/openai-fixture/"
podman cp "${E2E_DIR}/openai-fixture" "node-0:/fixtures-openai"

echo "registering the openai wasm extension from node-0..."
LOAD_OUTPUT="$(run_in_node 0 lycoris cluster ext load /fixtures-openai/openai.pkg.toml)"
echo "${LOAD_OUTPUT}"
if ! echo "${LOAD_OUTPUT}" | grep -qE "accepted:.*true"; then
  echo "error: openai extension registration was not accepted" >&2
  exit 1
fi
echo "ok: openai extension registered"

echo "waiting for the openai extension to converge to node-1..."
if ! wait_until_extension_visible 1 "openai"; then
  echo "error: openai did not become visible on node-1" >&2
  run_in_node 1 lycoris cluster get extensions >&2 || true
  exit 1
fi

echo "waiting for node-1 to announce the ext.openai capability..."
if ! wait_until_extension_announced 0 "node-1" "openai"; then
  echo "error: node-1 did not announce ext.openai in time" >&2
  run_in_node 0 lycoris cluster get node node-1 >&2 || true
  exit 1
fi
echo "ok: node-1 runs the openai provider and advertises it"

echo "invoking openai chat from node-0 (must route to node-1)..."
CHAT_STDERR="${E2E_DIR}/openai-chat.stderr"
if ! CHAT_OUTPUT="$(run_in_node 0 lycoris cluster ext invoke openai chat \
  '{"model":"gpt-mock","messages":[{"role":"user","content":"hi"}]}' 2>"${CHAT_STDERR}")"; then
  echo "error: openai chat invocation failed" >&2
  cat "${CHAT_STDERR}" >&2
  exit 1
fi
echo "${CHAT_OUTPUT}"
if ! echo "${CHAT_OUTPUT}" | grep -q "canned hello"; then
  echo "error: chat response does not carry the canned completion" >&2
  exit 1
fi
if ! grep -q "executed by: node-1" "${CHAT_STDERR}"; then
  echo "error: the chat call was not routed to node-1" >&2
  cat "${CHAT_STDERR}" >&2
  exit 1
fi
echo "ok: chat routed to node-1 and answered by the mock through the wasm guest"

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
