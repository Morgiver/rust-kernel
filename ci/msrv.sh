#!/usr/bin/env bash
# Print the minimum supported Rust version DECLARED in the workspace manifest.
#
# Every guard that needs the MSRV calls this script. None of them hardcodes a
# version: a hardcoded version silently stops tracking the manifest, which is
# exactly how a wrong `rust-version` survived two waves unnoticed.
#
# Reads `[workspace.package] rust-version` with awk only, so it runs before any
# toolchain is installed.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$root/Cargo.toml"

version="$(
  awk '
    /^[[:space:]]*\[/ { section = $0; next }
    section == "[workspace.package]" && /^[[:space:]]*rust-version[[:space:]]*=/ {
      line = $0
      sub(/^[^=]*=[[:space:]]*/, "", line)
      gsub(/["'"'"']/, "", line)
      sub(/[[:space:]]*(#.*)?$/, "", line)
      print line
      exit
    }
  ' "$manifest"
)"

if [ -z "$version" ]; then
  echo "guard: no rust-version under [workspace.package] in $manifest" >&2
  exit 1
fi

printf '%s\n' "$version"
