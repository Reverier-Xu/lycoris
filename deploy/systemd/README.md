# Lycoris systemd Deployment

This directory contains systemd unit files and an install helper for running the
lycoris server daemon as a persistent system service, either system-wide or
per-user.

> The server binary produced by this repository is named `lycoris-daemon`. The
> service itself is named `lycoris-server.service` to match the common role of the
> daemon.

## Files

- `lycoris-server.service` — system-wide systemd unit file.
- `lycoris-server.user.service` — per-user systemd unit file.
- `install.sh` — convenience installer that supports both modes.

## System-wide Install

1. Build the release binary:

   ```bash
   cargo build --release -p lycoris-daemon
   ```

2. Run the installer as root:

   ```bash
   sudo ./deploy/systemd/install.sh
   ```

3. Create the daemon configuration at `/etc/lycoris/lycoris.toml`.

4. Start and inspect the service:

   ```bash
   sudo systemctl start lycoris-server
   sudo systemctl status lycoris-server
   ```

The installer creates a dedicated `lycoris` system user, the configuration
directory `/etc/lycoris`, and the working/data directory `/var/lib/lycoris`.

## User-mode Install

User-mode services run under your own systemd user instance and do not require
root privileges.

1. Build the release binary:

   ```bash
   cargo build --release -p lycoris-daemon
   ```

2. Run the installer with the `--user` flag:

   ```bash
   ./deploy/systemd/install.sh --user
   ```

3. Create the daemon configuration at `~/.config/lycoris/lycoris.toml`.

4. Start and inspect the service:

   ```bash
   systemctl --user start lycoris-server
   systemctl --user status lycoris-server
   ```

The installer places the binary in `~/.local/bin`, the unit file in
`~/.config/systemd/user/lycoris-server.service`, and creates the configuration
and data directories under `~/.config/lycoris` and `~/.local/share/lycoris`.

If you want the service to start automatically on login, make sure your user
systemd instance is enabled (this is the default on most modern distributions).

## Manual Install

If you prefer to install manually, copy the appropriate unit file to the correct
systemd directory:

- System: `/etc/systemd/system/lycoris-server.service`
- User: `~/.config/systemd/user/lycoris-server.service`

Adjust the following paths as needed:

- `ExecStart` — path to the `lycoris-daemon` binary.
- `--config` — path to the daemon configuration file.
- `WorkingDirectory` — runtime data directory.
- `User` / `Group` — the account under which the daemon runs (system mode only).

After editing, reload systemd and enable the service:

```bash
# system mode
sudo systemctl daemon-reload
sudo systemctl enable --now lycoris-server

# user mode
systemctl --user daemon-reload
systemctl --user enable --now lycoris-server
```

## Service Management

```bash
# system mode
sudo systemctl start lycoris-server
sudo systemctl stop lycoris-server
sudo systemctl restart lycoris-server
sudo journalctl -u lycoris-server -f

# user mode
systemctl --user start lycoris-server
systemctl --user stop lycoris-server
systemctl --user restart lycoris-server
journalctl --user -u lycoris-server -f
```

## Hardening

The system-wide unit file enables several standard systemd sandboxing options. If
your deployment requires additional privileges (for example, binding to a
privileged port or accessing a custom certificate directory), edit the unit file
and add the required `AmbientCapabilities`, `ReadWritePaths`, or other directives.

The user-mode unit file intentionally uses a smaller sandboxing profile, because
user instances already run under the invoking user's privileges and some
sandboxing directives behave differently or are unavailable in user mode.
