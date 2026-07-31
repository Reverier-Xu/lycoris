#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/config"

mkdir -p "${CONFIG_DIR}"
rm -f "${CONFIG_DIR}"/node-*.toml

for node in node-0 node-1 node-2; do
  mkdir -p "${CONFIG_DIR}/${node}"
  rm -f "${CONFIG_DIR}/${node}/lycoris.toml"
  cat > "${CONFIG_DIR}/${node}/lycoris.toml" <<EOF
data_dir = "/var/lib/lycoris"

[node]
id = "${node}"
address = "https://${node}:5000"

[cluster]
listen_address = "0.0.0.0:5000"
bootstrap_peers = []
overlay_listen = ["/ip4/0.0.0.0/tcp/5001"]

[tls]
ca_cert = "/etc/lycoris/certs/ca.crt"
ca_key = "/etc/lycoris/certs/ca.key"
cert = "/etc/lycoris/certs/${node}.crt"
key = "/etc/lycoris/certs/${node}.key"
EOF
done

cat >> "${CONFIG_DIR}/node-1/lycoris.toml" <<'EOF'

[node.labels]
role = "runner"
EOF

echo "generated configs in ${CONFIG_DIR}"
