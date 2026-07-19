# lycoris-storage

`lycoris-storage` provides the persistence layer for Lycoris nodes.

## Responsibilities

- Stores node-local metadata, peer information, and workspace indexes.
- Manages shared skills and rules: loading, versioning, content validation, and synchronization.
- Manages extension packages: `ExtensionRecord` metadata in redb plus a filesystem blob store for artifact bytes, written blob-before-metadata so a failed write never leaves dangling metadata; both the anti-entropy apply pipeline and the admission-side registration path (`ExtensionManager::register`) persist through the same `apply_remote_extension` entry point.
- Provides vector storage for long-term memory retrieval; memory records now carry a `version` field and share the unified version model with skill/rule/workspace.
- Provides the `VersionedRecord` trait and the `should_apply_versioned` conflict-resolution helper, so the anti-entropy engine can handle all resource types uniformly.

## Main Modules

- `node`: node-level metadata and peer state storage.
- `workspace`: storage and version management for workspace, skill, and rule.
- `agent`: agent memory and related persistence structures.
- `extension`: extension package metadata and artifact blob store.

## Backends

- `redb`: local key-value/table-structured metadata storage.
- `lancedb`: vector data storage.
