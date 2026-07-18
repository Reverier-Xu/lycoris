# lycoris (shell)

`lycoris` is the unified binary entry point of Lycoris, dispatching to different execution modes via subcommands.

## Subcommands

- `lycoris daemon`: starts the cluster node daemon.
- `lycoris cluster`: inspects and operates on cluster membership state.
- `lycoris setup`: installs the platform service unit (systemd user service on linux, launchd agent on macOS) for the daemon. It does not generate node configuration, TLS certificates, or the cluster key — create those separately (see `lycoris cluster init`).

To run the daemon in the background, prefer `lycoris setup` plus the platform service manager (`systemctl --user`, `launchctl`) over hand-rolled backgrounding.

## Design Notes

The shell itself only handles command parsing, configuration loading, and invoking `lycoris-daemon` or `lycoris-client`. The actual client communication logic is provided by `lycoris-client`. `lycoris` and `lycoris-daemon` share the same working directory and data directory conventions.
