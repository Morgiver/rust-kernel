#!/usr/bin/env bash
# Guard: the lint configuration that holds the other boundaries is still there.
#
# `missing_docs = "deny"` is what makes the rustdoc job mean anything, and
# `unsafe_code = "forbid"` is what makes "no unsafe, anywhere" a fact rather
# than a habit. Both live in `[workspace.lints]`, and both stop applying the
# moment a crate drops `[lints] workspace = true` or a module writes an
# `allow`. That silence is what this guard breaks.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

failures=0
fail() {
  printf 'lint guard: %s\n' "$1" >&2
  failures=$((failures + 1))
}

require_line() {
  local pattern="$1" file="$2" description="$3"
  grep -qE -- "$pattern" "$file" || fail "$file no longer declares $description"
}

require_line '^missing_docs[[:space:]]*=[[:space:]]*"deny"' Cargo.toml 'missing_docs = "deny"'
require_line '^unsafe_code[[:space:]]*=[[:space:]]*"forbid"' Cargo.toml 'unsafe_code = "forbid"'
require_line '^all[[:space:]]*=.*level[[:space:]]*=[[:space:]]*"deny"' Cargo.toml 'clippy::all at deny'

for manifest in crates/*/Cargo.toml; do
  awk '
    /^[[:space:]]*\[/ { section = $0 }
    section == "[lints]" && /^[[:space:]]*workspace[[:space:]]*=[[:space:]]*true/ { found = 1 }
    END { exit found ? 0 : 1 }
  ' "$manifest" || fail "$manifest does not inherit the workspace lints ([lints] workspace = true)"
done

# An `allow` on either lint puts the boundary back to a matter of vigilance.
while IFS= read -r hit; do
  [ -n "$hit" ] || continue
  fail "lint override: $hit"
done < <(grep -rnE '#!?\[allow\((missing_docs|unsafe_code)' --include='*.rs' crates || true)

# `unsafe_code = "forbid"` already refuses these, but the grep says so without
# waiting for a compiler that someone may have reconfigured.
while IFS= read -r hit; do
  [ -n "$hit" ] || continue
  fail "unsafe keyword: $hit"
done < <(grep -rnw 'unsafe' --include='*.rs' crates || true)

if [ "$failures" -ne 0 ]; then
  echo "lint guard: $failures violation(s)" >&2
  exit 1
fi

echo "lint guard: ok"
