#!/usr/bin/env bash
# Guard: dependency allowlist (design section 16, "Le Kernel ne depend de rien
# de metier").
#
# The rule is an allowlist, not a denylist: a crate of the kernel may depend on
# what this table names and on nothing else. A workspace member absent from the
# table fails the guard on purpose — adding a dependency surface to the kernel
# is a decision, and this file is where the decision is recorded.
#
# Table format, one entry per workspace member:
#   <package name>|<comma separated allowed direct dependencies>
# An empty right-hand side means "no dependency of any kind". The value `*`
# lifts the restriction for a member that is not part of the kernel itself.
#
# The check covers normal, dev and build dependencies. `kernel-core` is checked
# transitively as well: it must resolve to itself alone.
set -euo pipefail

ALLOWLIST=(
  "kernel-core|"
  "kernel|kernel-core,tokio"
  "kernel-macros|kernel-core,kernel,tokio"
  "kernel-testkit|kernel-core,kernel,tokio"
  # Not part of the kernel: an application-layer illustration, free to depend on
  # whatever an application would.
  "minimal|*"
)

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

failures=0
fail() {
  printf 'dependency guard: %s\n' "$1" >&2
  failures=$((failures + 1))
}

# `--locked` is deliberately absent from the cargo calls this guard reads. A
# dependency added without regenerating the lockfile makes cargo exit 101 before
# any of the checks below run, and the operator reads a lockfile error instead of
# the allowlist violation that caused it. The lockfile is still checked, and the
# build still fails on it — but the guard speaks first.
#
# The probe runs here, before anything unlocked has had a chance to rewrite the
# lockfile, and its verdict is reported at the end. The lockfile itself is put
# back afterwards, so a guard that reports a stale lockfile does not quietly fix
# the very thing it is reporting.
lock_error=""
if ! lock_error="$(cargo metadata --format-version 1 --locked 2>&1 >/dev/null)"; then
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

metadata="$(cargo metadata --format-version 1 --no-deps)"
declared_msrv="$("$root/ci/msrv.sh")"

allowed_for() {
  local package="$1" entry
  for entry in "${ALLOWLIST[@]}"; do
    if [ "${entry%%|*}" = "$package" ]; then
      printf '%s\n' "${entry#*|}"
      return 0
    fi
  done
  return 1
}

members="$(printf '%s' "$metadata" | jq -r '.packages[].name' | sort)"

while IFS= read -r package; do
  [ -n "$package" ] || continue

  if ! allowed=$(allowed_for "$package"); then
    fail "workspace member '$package' has no entry in the allowlist of ci/check-dependencies.sh"
    continue
  fi

  if [ "$allowed" = "*" ]; then
    continue
  fi

  # rust-version drift: every member of the kernel must carry the declared MSRV.
  member_msrv="$(printf '%s' "$metadata" | jq -r --arg p "$package" '.packages[] | select(.name == $p) | .rust_version // ""')"
  if [ "$member_msrv" != "$declared_msrv" ]; then
    fail "'$package' declares rust-version '$member_msrv', workspace declares '$declared_msrv'"
  fi

  while IFS=$'\t' read -r kind dependency; do
    [ -n "$dependency" ] || continue
    case ",$allowed," in
      *",$dependency,"*) ;;
      *) fail "'$package' depends on '$dependency' ($kind), which the allowlist does not permit" ;;
    esac
  done < <(printf '%s' "$metadata" | jq -r --arg p "$package" '
    .packages[] | select(.name == $p) | .dependencies[] | "\(.kind // "normal")\t\(.name)"
  ')
done <<< "$members"

# kernel-core must have no external dependency at all, transitively included.
if printf '%s' "$members" | grep -qx 'kernel-core'; then
  # Stderr goes to a file rather than into `resolved`: without `--locked`, cargo
  # narrates what it is doing there, and the narration would be read as packages.
  tree_log="$(mktemp)"
  if ! resolved="$(cargo tree --package kernel-core --edges normal,build,dev --prefix none --no-dedupe --format '{p}' 2>"$tree_log")"; then
    fail "cargo tree failed on kernel-core:"$'\n'"$(cat "$tree_log")"
    rm -f "$tree_log"
  else
    rm -f "$tree_log"
    resolved="$(printf '%s\n' "$resolved" | awk 'NF {print $1}' | sort -u)"
    if [ "$resolved" != "kernel-core" ]; then
      fail "kernel-core resolves to more than itself:"$'\n'"$resolved"
    fi
  fi
fi

# Reported last, so that whatever moved the lockfile has already been named.
if [ -n "$lock_error" ]; then
  fail "the lockfile is out of date:"$'\n'"$lock_error"
fi

if [ "$failures" -ne 0 ]; then
  echo "dependency guard: $failures violation(s)" >&2
  exit 1
fi

echo "dependency guard: ok"
