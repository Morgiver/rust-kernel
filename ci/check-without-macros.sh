#!/usr/bin/env bash
# Guard: the suite builds and passes with the macros crate absent (design
# section 15, "le Kernel doit compiler et fonctionner avec le crate de macros
# retire").
#
# Every macro the kernel ships expands to a public API the user could have
# written by hand, so no macro may ever become load bearing. The guard removes
# every workspace member whose name ends in `-macros` from the build and runs
# the suite without it. A macros crate reachable from a non-macros member with
# default features fails the guard: reachable means required.
#
# Third-party proc-macro crates (`tokio-macros` and friends) are not concerned;
# only workspace members are.
#
# No `*-macros` member exists today, so the guard reduces to "the suite builds",
# and it starts biting the moment one is added.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

members="$(cargo metadata --format-version 1 --no-deps --locked | jq -r '.packages[].name')"
macro_members="$(printf '%s\n' "$members" | { grep -E -- '-macros$' || true; })"

if [ -n "$macro_members" ]; then
  reachable="$(
    cargo tree --workspace --exclude '*-macros' --edges normal,build,dev \
      --prefix none --format '{p}' --locked 2>/dev/null | awk '{print $1}' | sort -u
  )"
  offenders=""
  while IFS= read -r macro_crate; do
    [ -n "$macro_crate" ] || continue
    if printf '%s\n' "$reachable" | grep -qx -- "$macro_crate"; then
      offenders="$offenders $macro_crate"
    fi
  done <<< "$macro_members"
  if [ -n "$offenders" ]; then
    echo "macro guard: these macros crates are reachable with default features:$offenders" >&2
    echo "macro guard: a macros crate must stay optional and off by default" >&2
    exit 1
  fi
fi

cargo test --workspace --exclude '*-macros' --locked

echo "macro guard: ok (workspace macros crates: ${macro_members:-none})"
