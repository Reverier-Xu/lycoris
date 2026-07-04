#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/config"

rm -rf "${CONFIG_DIR}"
mkdir -p "${CONFIG_DIR}"

cat > "${CONFIG_DIR}/node-0.toml" <<'EOF'
data_dir = "/var/lib/lycoris"

[node]
id = "node-0"
address = "https://node-0:5001"

[cluster]
listen_address = "0.0.0.0:5001"
bootstrap_peers = ["https://node-1:5001"]

[tls]
ca_cert = "/etc/lycoris/certs/ca.crt"
ca_key = "/etc/lycoris/certs/ca.key"
cert = "/etc/lycoris/certs/node-0.crt"
key = "/etc/lycoris/certs/node-0.key"
EOF

cat > "${CONFIG_DIR}/node-1.toml" <<'EOF'
data_dir = "/var/lib/lycoris"

[node]
id = "node-1"
address = "https://node-1:5001"

[cluster]
listen_address = "0.0.0.0:5001"
bootstrap_peers = ["https://node-0:5001", "https://node-2:5001"]

[tls]
ca_cert = "/etc/lycoris/certs/ca.crt"
ca_key = "/etc/lycoris/certs/ca.key"
cert = "/etc/lycoris/certs/node-1.crt"
key = "/etc/lycoris/certs/node-1.key"
EOF

cat > "${CONFIG_DIR}/node-2.toml" <<'EOF'
data_dir = "/var/lib/lycoris"

[node]
id = "node-2"
address = "https://node-2:5001"

[cluster]
listen_address = "0.0.0.0:5001"
bootstrap_peers = ["https://node-1:5001"]

[tls]
ca_cert = "/etc/lycoris/certs/ca.crt"
ca_key = "/etc/lycoris/certs/ca.key"
cert = "/etc/lycoris/certs/node-2.crt"
key = "/etc/lycoris/certs/node-2.key"
EOF

echo "generated configs in ${CONFIG_DIR}"
