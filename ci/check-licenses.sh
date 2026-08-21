#!/usr/bin/env bash
# Guard: the repository actually grants the licence it claims.
#
# `Cargo.toml` claims `MIT OR Apache-2.0`. A claim with no text next to it
# grants nothing, which is the state this repository was published in. The
# guard reads the claim, resolves each named licence to its file and fails if
# one is missing, empty, or no longer carries a copyright line.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

failures=0
fail() {
  printf 'licence guard: %s\n' "$1" >&2
  failures=$((failures + 1))
}

claim="$(
  awk '
    /^[[:space:]]*\[/ { section = $0; next }
    section == "[workspace.package]" && /^[[:space:]]*license[[:space:]]*=/ {
      line = $0
      sub(/^[^=]*=[[:space:]]*/, "", line)
      gsub(/"/, "", line)
      sub(/[[:space:]]*(#.*)?$/, "", line)
      print line
      exit
    }
  ' Cargo.toml
)"

[ -n "$claim" ] || fail 'no license field under [workspace.package]'

file_for() {
  case "$1" in
    MIT) printf 'LICENSE-MIT\n' ;;
    Apache-2.0) printf 'LICENSE-APACHE\n' ;;
    *) return 1 ;;
  esac
}

for licence in $(printf '%s\n' "$claim" | tr ' ' '\n' | grep -v '^\(OR\|AND\|WITH\)$'); do
  if ! file="$(file_for "$licence")"; then
    fail "no file is mapped to '$licence'; add the mapping to ci/check-licenses.sh"
    continue
  fi
  if [ ! -s "$file" ]; then
    fail "Cargo.toml claims '$licence' but $file is missing or empty"
    continue
  fi
  grep -qi 'copyright' "$file" || fail "$file carries no copyright line"
done

if [ "$failures" -ne 0 ]; then
  echo "licence guard: $failures violation(s)" >&2
  exit 1
fi

echo "licence guard: ok ($claim)"
