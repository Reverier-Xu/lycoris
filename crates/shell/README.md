# lycoris (shell)

`lycoris` is the unified binary entry point of Lycoris, dispatching to different execution modes via subcommands.

## Subcommands

- `lycoris daemon`: starts the cluster node daemon.
- `lycoris cluster`: inspects and operates on cluster membership state.
- `lycoris setup`: initializes node configuration, TLS certificates, and the cluster key.

## Design Notes

The shell itself only handles command parsing, configuration loading, and invoking `lycoris-daemon` or `lycoris-client`. The actual client communication logic is provided by `lycoris-client`. `lycoris` and `lycoris-daemon` share the same working directory and data directory conventions.
