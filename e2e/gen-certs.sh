#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERTS_DIR="${SCRIPT_DIR}/certs"
NODES=(node-0 node-1 node-2)

rm -rf "${CERTS_DIR}"
mkdir -p "${CERTS_DIR}"
cd "${CERTS_DIR}"

# CA
openssl req -x509 -newkey rsa:4096 -keyout ca.key -out ca.crt -days 1 -nodes \
  -subj "/CN=lycoris-e2e-ca" \
  2>/dev/null

for node in "${NODES[@]}"; do
  openssl req -newkey rsa:4096 -keyout "${node}.key" -out "${node}.csr" -nodes \
    -subj "/CN=${node}" \
    -addext "subjectAltName=DNS:${node}" \
    2>/dev/null
  openssl x509 -req -in "${node}.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "${node}.crt" -days 1 \
    -copy_extensions copy \
    2>/dev/null
  rm -f "${node}.csr"
done

rm -f ca.srl
echo "generated certificates in ${CERTS_DIR}"
