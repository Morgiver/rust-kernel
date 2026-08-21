#!/usr/bin/env bash
# Guard: no production dependency edge enables `kernel/testing`.
#
# `kernel/testing` exposes `KernelBuilder::__register_hook`, the low-level hook
# `kernel-testkit` substitutes bindings through. The guarantee this guard holds
# is narrow and worth stating exactly:
#
#   In a PRODUCTION build — a dependency graph that reaches `kernel` without
#   passing through a dev-dependency on `kernel-testkit` — the feature is off,
#   `__register_hook` does not exist, and no substitution is reachable.
#
# What it does NOT hold: inside `cargo test` of a crate that dev-depends on
# `kernel-testkit`, cargo unifies features across the build, `kernel/testing`
# is on for the whole graph, and any test in that crate can call
# `KernelBuilder::new().__register_hook(...)` with no kernel-testkit type in
# scope. That was verified by experiment, not assumed. It is acceptable — the
# person reaching it is writing tests — but it is not held by the type system,
# and nothing in this repository may claim that it is.
#
# The check has two halves, because one alone has a hole:
#
#   1. Resolved: for every workspace member, `cargo tree --edges normal`
#      resolves the feature set a production build of that member would get.
#      A `kernel` node carrying `testing` there is a live violation, whether
#      the member enabled it directly or reached it through another crate —
#      putting `kernel-testkit` in `[dependencies]` instead of
#      `[dev-dependencies]` lands here.
#   2. Declared: a member may enable the feature behind an optional feature of
#      its own, which the resolved tree does not activate today and would
#      activate the day someone turns that feature on. The declared dependency
#      table is read directly, so the violation is caught while it is still
#      dormant.
#
# Build dependencies are out of scope on purpose: with resolver v2 semantics
# they are resolved on their own feature graph and nothing they enable reaches
# the crate that ships.
set -euo pipefail

# The crate that owns the feature, and the feature itself.
CRATE="kernel"
FEATURE="testing"

# The one exemption, and the reason it is one: `kernel-testkit` IS the test
# harness. It enables the feature as a normal dependency because its whole
# public surface — `TestBuilder` and its substitution verbs — is built on the
# hook, and a crate reaches it the ordinary way, as a dev-dependency, which
# keeps it out of the production graph of whatever depends on that crate.
EXEMPT="kernel-testkit"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

failures=0
fail() {
  printf 'testing-feature guard: %s\n' "$1" >&2
  failures=$((failures + 1))
}

members="$(cargo metadata --format-version 1 --no-deps | jq -r '.packages[].name' | sort)"

# ---------------------------------------------------------------------------
# 1. Resolved: what a production build of each member actually turns on.
# ---------------------------------------------------------------------------
while IFS= read -r member; do
  [ -n "$member" ] || continue
  if [ "$member" = "$EXEMPT" ]; then
    continue
  fi

  tree_log="$(mktemp)"
  if ! tree="$(cargo tree --package "$member" --edges normal --prefix none \
      --format '{p}|{f}' 2>"$tree_log")"; then
    fail "cargo tree failed on '$member':"$'\n'"$(cat "$tree_log")"
    rm -f "$tree_log"
    continue
  fi
  rm -f "$tree_log"

  while IFS='|' read -r package features; do
    [ -n "$package" ] || continue
    [ "${package%% *}" = "$CRATE" ] || continue
    case ",$features," in
      *",$FEATURE,"*)
        fail "'$member' resolves '$CRATE' with '$FEATURE' on through a normal dependency (features: $features)"
        ;;
    esac
  done <<< "$tree"
done <<< "$members"

# ---------------------------------------------------------------------------
# 2. Declared: an enabling written down but not currently activated.
# ---------------------------------------------------------------------------
while IFS=$'\t' read -r member dependency; do
  [ -n "$member" ] || continue
  [ "$member" = "$EXEMPT" ] && continue
  fail "'$member' declares dependency '$dependency' with '$CRATE/$FEATURE' enabled, and it is not a dev-dependency"
done < <(cargo metadata --format-version 1 --no-deps | jq -r --arg crate "$CRATE" --arg feature "$FEATURE" '
  .packages[]
  | .name as $member
  | .dependencies[]
  | select((.kind // "normal") != "dev")
  | select(.name == $crate and ((.features // []) | index($feature)))
  | "\($member)\t\(.name)"
')

# ---------------------------------------------------------------------------
# 3. Optional: an enabling parked in a member's own feature table.
#
# `debugging = ["kernel/testing"]` activates nothing until someone builds with
# `--features debugging`, so neither of the checks above sees it — and cargo
# refuses `dep/feature` for a dev-dependency, so anything written there is by
# construction an enabling on the production side of the graph.
# ---------------------------------------------------------------------------
while IFS=$'\t' read -r member feature_name expansion; do
  [ -n "$member" ] || continue
  [ "$member" = "$EXEMPT" ] && continue
  fail "'$member' defines feature '$feature_name = [\"$expansion\"]', which enables '$CRATE/$FEATURE' on a non-dev dependency"
done < <(cargo metadata --format-version 1 --no-deps | jq -r --arg crate "$CRATE" --arg feature "$FEATURE" '
  .packages[]
  | .name as $member
  | (.features // {})
  | to_entries[]
  | .key as $name
  | .value[]
  | select(. == "\($crate)/\($feature)" or . == "\($crate)?/\($feature)")
  | "\($member)\t\($name)\t\(.)"
')

if [ "$failures" -ne 0 ]; then
  echo "testing-feature guard: $failures violation(s)" >&2
  echo "testing-feature guard: '$CRATE/$FEATURE' belongs behind a dev-dependency on '$EXEMPT', nowhere else" >&2
  exit 1
fi

echo "testing-feature guard: ok ('$CRATE/$FEATURE' off in every production graph; '$EXEMPT' exempt)"
