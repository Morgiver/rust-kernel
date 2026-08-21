#!/usr/bin/env bash
# Guard: no `*-bundle` crate depends on another `*-bundle` crate (design
# section 16, "Bundle <-> Bundle interdit").
#
# Bundles talk to each other through contracts resolved from the container, and
# through nothing else. A direct crate dependency between two bundles is the
# coupling the design forbids: it makes one bundle unusable without the other
# and reintroduces the ordering the phases exist to remove.
#
# No such crate exists in this repository today, so the guard passes vacuously.
# It is written for the day one appears, and it looks at the whole resolved
# graph — a bundle pulled from a registry is checked exactly like a workspace
# member.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# `--locked` is deliberately absent from the two calls below. A cross-bundle
# dependency also moves the lockfile, so `--locked` would make cargo exit 101
# before any of this runs and the operator would read a lockfile error instead of
# the coupling that caused it. The lockfile is still checked, and the build still
# fails on it — but the guard speaks first.
#
# The probe runs here, before anything unlocked can rewrite the lockfile, and the
# lockfile is put back afterwards: a guard that reports a stale lockfile must not
# quietly fix the thing it is reporting.
lock_error=""
if ! lock_error="$(cargo metadata --format-version 1 --locked --all-features 2>&1 >/dev/null)"; then
  lock_error="${lock_error:-cargo refused the lockfile}"
  if [ -f Cargo.lock ]; then
    lock_backup="$(mktemp)"
    cp Cargo.lock "$lock_backup"
    # shellcheck disable=SC2064
    trap "cp '$lock_backup' Cargo.lock; rm -f '$lock_backup'" EXIT
  fi
else
  lock_error=""
fi

pairs="$(
  cargo metadata --format-version 1 --all-features | jq -r '
    (.packages | map({key: .id, value: .name}) | from_entries) as $names
    | [.packages[] | select(.name | test("-bundle$")) | .name] as $bundles
    | .resolve.nodes[]
    | $names[.id] as $from
    | select($from | test("-bundle$"))
    | .deps[]
    | $names[.pkg] as $to
    | select($to | test("-bundle$"))
    | "\($from) -> \($to)"
  '
)"

if [ -n "$pairs" ]; then
  echo "bundle graph guard: a bundle depends on another bundle" >&2
  while IFS= read -r pair; do
    [ -n "$pair" ] && printf '  %s\n' "$pair" >&2
  done <<< "$pairs"
  echo "bundle graph guard: cross-bundle coupling belongs in a contract, not in Cargo.toml" >&2
  exit 1
fi

if [ -n "$lock_error" ]; then
  echo "bundle graph guard: the lockfile is out of date" >&2
  printf '%s\n' "$lock_error" >&2
  exit 1
fi

count="$(cargo metadata --format-version 1 --all-features | jq -r '[.packages[] | select(.name | test("-bundle$"))] | length')"
echo "bundle graph guard: ok ($count bundle crate(s) in the graph)"
