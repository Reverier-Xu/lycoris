# Lycoris systemd Deployment

This directory contains systemd unit files and an install helper for running the
lycoris server daemon as a persistent system service, either system-wide or
per-user.

> The server binary produced by this repository is named `lycoris` and is run with `lycoris daemon`. The
> systemd service itself is named `lycoris.service`.

## Files

- `lycoris.service` — system-wide systemd unit file.
- `lycoris.user.service` — per-user systemd unit file.
- `install.sh` — convenience installer that supports both modes.

## System-wide Install

1. Build the release binary:

   ```bash
   cargo build --release -p lycoris
   ```

2. Run the installer as root:

   ```bash
   sudo ./deploy/systemd/install.sh
   ```

3. Create the daemon configuration at `/etc/lycoris/lycoris.toml`.

4. Start and inspect the service:

   ```bash
   sudo systemctl start lycoris
   sudo systemctl status lycoris
   ```

The installer creates a dedicated `lycoris` system user, the configuration
directory `/etc/lycoris`, and the working/data directory `/var/lib/lycoris`.

## User-mode Install

User-mode services run under your own systemd user instance and do not require
root privileges.

1. Build the release binary:

   ```bash
   cargo build --release -p lycoris
   ```

2. Run the installer with the `--user` flag:

   ```bash
   ./deploy/systemd/install.sh --user
   ```

3. Create the daemon configuration at `~/.config/lycoris/lycoris.toml`.

4. Start and inspect the service:

   ```bash
   systemctl --user start lycoris
   systemctl --user status lycoris
   ```

The installer places the binary in `~/.local/bin`, the unit file in
`~/.config/systemd/user/lycoris.service`, and creates the configuration
and data directories under `~/.config/lycoris` and `~/.local/share/lycoris`.

If you want the service to start automatically on login, make sure your user
systemd instance is enabled (this is the default on most modern distributions).

## Manual Install

If you prefer to install manually, copy the appropriate unit file to the correct
systemd directory:

- System: `/etc/systemd/system/lycoris.service`
- User: `~/.config/systemd/user/lycoris.service`

Adjust the following paths as needed:

- `ExecStart` — path to the `lycoris daemon` binary.
- `--config` — path to the daemon configuration file.
- `WorkingDirectory` — runtime data directory.
- `User` / `Group` — the account under which the daemon runs (system mode only).

After editing, reload systemd and enable the service:

```bash
# system mode
sudo systemctl daemon-reload
sudo systemctl enable --now lycoris

# user mode
systemctl --user daemon-reload
systemctl --user enable --now lycoris
```

## Service Management

```bash
# system mode
sudo systemctl start lycoris
sudo systemctl stop lycoris
sudo systemctl restart lycoris
sudo journalctl -u lycoris -f

# user mode
systemctl --user start lycoris
systemctl --user stop lycoris
systemctl --user restart lycoris
journalctl --user -u lycoris -f
```

## Hardening

The system-wide unit file enables several standard systemd sandboxing options. If
your deployment requires additional privileges (for example, binding to a
privileged port or accessing a custom certificate directory), edit the unit file
and add the required `AmbientCapabilities`, `ReadWritePaths`, or other directives.

The user-mode unit file intentionally uses a smaller sandboxing profile, because
user instances already run under the invoking user's privileges and some
sandboxing directives behave differently or are unavailable in user mode.
