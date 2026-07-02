#!/usr/bin/env bash
set -euo pipefail

# Install lycoris-server as a systemd service.
# Supports both system-wide (root) and per-user (--user) installations.

SERVICE_NAME="lycoris"
BINARY_NAME="lycoris-daemon"

user_mode=false
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --user)
      user_mode=true
      shift
      ;;
    -h | --help)
      cat <<EOF
usage: $0 [--user]

  --user    install as a user-mode systemd service instead of system-wide
EOF
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      echo "usage: $0 [--user]" >&2
      exit 1
      ;;
  esac
done

if ! command -v systemctl >/dev/null 2>&1; then
  echo "error: systemctl is not available" >&2
  exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "${user_mode}" == true ]]; then
  home="${HOME:-$(getent passwd "$(id -u)" | cut -d: -f6)}"
  if [[ -z "${home}" ]]; then
    echo "error: could not determine home directory" >&2
    exit 1
  fi

  install_prefix="${INSTALL_PREFIX:-${home}/.local}"
  config_dir="${home}/.config/lycoris"
  data_dir="${home}/.local/share/lycoris"
  systemd_user_dir="${home}/.config/systemd/user"
  service_file="lycoris.user.service"
  systemd_args=("--user")
else
  if [[ "$(id -u)" -ne 0 ]]; then
    echo "error: system-wide install must be run as root (use --user for a user install)" >&2
    exit 1
  fi

  install_prefix="${INSTALL_PREFIX:-/usr/local}"
  config_dir="/etc/lycoris"
  data_dir="/var/lib/lycoris"
  systemd_user_dir="/etc/systemd/system"
  service_file="lycoris.service"
  systemd_args=()
fi

# Install the binary if it exists next to the script or in cargo target/release.
binary_path=""
if [[ -x "${script_dir}/${BINARY_NAME}" ]]; then
  binary_path="${script_dir}/${BINARY_NAME}"
elif [[ -x "${script_dir}/../../target/release/${BINARY_NAME}" ]]; then
  binary_path="${script_dir}/../../target/release/${BINARY_NAME}"
elif command -v "${BINARY_NAME}" >/dev/null 2>&1; then
  binary_path="$(command -v "${BINARY_NAME}")"
fi

if [[ -z "${binary_path}" ]]; then
  echo "error: could not find '${BINARY_NAME}' binary" >&2
  echo "       place it next to this script, build it with 'cargo build --release -p lycoris-daemon', or install it in PATH" >&2
  exit 1
fi

echo "installing ${BINARY_NAME} from ${binary_path}..."
mkdir -p "${install_prefix}/bin"
cp -f "${binary_path}" "${install_prefix}/bin/${BINARY_NAME}"
chmod 755 "${install_prefix}/bin/${BINARY_NAME}"

# Create config and data directories.
mkdir -p "${config_dir}" "${data_dir}"

if [[ "${user_mode}" != true ]]; then
  # Create the dedicated user and group for the system-wide service.
  if ! id -u lycoris >/dev/null 2>&1; then
    echo "creating lycoris user..."
    useradd --system --no-create-home --home-dir "${data_dir}" --shell /usr/sbin/nologin lycoris
  fi
  chown -R lycoris:lycoris "${data_dir}"
  chmod 750 "${data_dir}"
fi

# Install the systemd unit file.
echo "installing systemd service..."
mkdir -p "${systemd_user_dir}"
cp -f "${script_dir}/${service_file}" "${systemd_user_dir}/${SERVICE_NAME}.service"
chmod 644 "${systemd_user_dir}/${SERVICE_NAME}.service"

# Reload systemd and enable the service.
systemctl "${systemd_args[@]}" daemon-reload
systemctl "${systemd_args[@]}" enable "${SERVICE_NAME}.service"

echo ""
if [[ "${user_mode}" == true ]]; then
  echo "lycoris has been installed as a user-mode systemd service."
  echo ""
  echo "next steps:"
  echo "  1. create or edit the daemon configuration at ${config_dir}/lycoris.toml"
  echo "  2. run: systemctl --user start ${SERVICE_NAME}.service"
  echo "  3. check status with: systemctl --user status ${SERVICE_NAME}.service"
else
  echo "lycoris has been installed as a system-wide systemd service."
  echo ""
  echo "next steps:"
  echo "  1. create or edit the daemon configuration at ${config_dir}/lycoris.toml"
  echo "  2. run: systemctl start ${SERVICE_NAME}.service"
  echo "  3. check status with: systemctl status ${SERVICE_NAME}.service"
fi
