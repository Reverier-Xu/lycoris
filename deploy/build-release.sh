#!/usr/bin/env bash
# build-release.sh — build the release artifacts of this workspace.
#
# usage:
#   deploy/build-release.sh bin <target-triple>
#       Build the lycoris binary for <target-triple> and package it as
#       dist/lycoris-<target-triple>.tar.gz with a .sha256 next to it
#       (a .zip on windows targets). The asset layout is what the
#       [package.metadata.binstall] section of crates/shell/Cargo.toml
#       points at.
#
#   deploy/build-release.sh wasm
#       Build every wasm extension (WASM_EXTENSIONS below) for
#       wasm32-unknown-unknown and copy the .wasm artifacts plus their
#       .sha256 into dist/. Wasm guests are platform-independent, so one
#       build serves every release target.
#
# The release workflow (.github/workflows/github_release.yml) calls this
# script once per platform; locally it reproduces the exact CI artifacts.
#
# Environment:
#   DIST_DIR          output directory (default: <repo>/dist)
#   CARGO_TARGET_DIR  cargo target dir (default: <repo>/target)
set -euo pipefail

# Wasm extensions published with every release: the crates under extensions/
# whose cdylib artifact the daemon loads through the wasm engine.
WASM_EXTENSIONS=(lycoris-ext-openai)

WASM_TARGET="wasm32-unknown-unknown"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="${DIST_DIR:-${ROOT}/dist}"
TARGET_DIR="${CARGO_TARGET_DIR:-${ROOT}/target}"

log() { printf '==> %s\n' "$*"; }
die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

# sha256sum on linux, shasum -a 256 on macOS.
sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$@"
	else
		shasum -a 256 "$@"
	fi
}

# Zip $2 (a bare file name in the cwd) into $1. GitHub windows runners
# have no `zip` in Git Bash but do have `7z`; accept either.
zip_archive() {
	if command -v zip >/dev/null 2>&1; then
		zip -j -q "$1" "$2"
	elif command -v 7z >/dev/null 2>&1; then
		7z a -tzip -bd -y "$1" "$2" >/dev/null
	else
		die "neither zip nor 7z is available to create $1"
	fi
}

build_bin() {
	local target="$1"
	log "building lycoris for ${target}"
	cargo build --release --locked --target "${target}" --package lycoris

	local bin_name="lycoris"
	case "${target}" in
	*windows*) bin_name="lycoris.exe" ;;
	esac
	local bin="${TARGET_DIR}/${target}/release/${bin_name}"
	[[ -f "${bin}" ]] || die "expected the lycoris binary at ${bin}"

	mkdir -p "${DIST}"
	local stage archive
	stage="$(mktemp -d)"
	cp "${bin}" "${stage}/${bin_name}"
	case "${target}" in
	*windows*)
		archive="lycoris-${target}.zip"
		(cd "${stage}" && zip_archive "${DIST}/${archive}" "${bin_name}")
		;;
	*)
		archive="lycoris-${target}.tar.gz"
		tar -czf "${DIST}/${archive}" -C "${stage}" "${bin_name}"
		;;
	esac
	rm -rf "${stage}"
	(cd "${DIST}" && sha256 "${archive}" >"${archive}.sha256")
	log "wrote ${DIST}/${archive}"
}

build_wasm() {
	local pkg lib artifact
	for pkg in "${WASM_EXTENSIONS[@]}"; do
		log "building ${pkg} for ${WASM_TARGET}"
		cargo build --release --locked --target "${WASM_TARGET}" --package "${pkg}"

		lib="${pkg//-/_}"
		artifact="${TARGET_DIR}/${WASM_TARGET}/release/${lib}.wasm"
		[[ -f "${artifact}" ]] || die "expected the wasm artifact at ${artifact}"

		mkdir -p "${DIST}"
		cp "${artifact}" "${DIST}/${pkg}.wasm"
		chmod 644 "${DIST}/${pkg}.wasm"
		(cd "${DIST}" && sha256 "${pkg}.wasm" >"${pkg}.wasm.sha256")
		log "wrote ${DIST}/${pkg}.wasm"
	done
}

usage() {
	sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//; $d'
}

case "${1:-}" in
bin)
	[[ -n "${2:-}" ]] || die "usage: $0 bin <target-triple>"
	build_bin "$2"
	;;
wasm)
	build_wasm
	;;
-h | --help)
	usage
	;;
*)
	usage >&2
	exit 1
	;;
esac
