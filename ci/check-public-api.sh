#!/usr/bin/env bash
# Guard: the public surface of the kernel crates matches the committed baseline.
#
# Two waves reshaped the public surface and nobody measured it. Three separate
# audits then found public items no caller could reach — a module exporting
# nothing, boot types no public API could produce, a `policy` accessor. Each
# was found by accident. This guard removes the accident: the surface is
# written down in `api/*.txt`, and any drift from it fails the build.
#
# WHAT THE BASELINE IS. A change detector. When the surface changes, the guard
# fails, and the only way past it is to regenerate the baseline in the same
# commit. The diff then sits in the review next to the code that caused it, so
# an item entering or leaving the surface is a thing someone decided rather
# than a thing that happened.
#
# WHAT IT IS NOT. Not a semver promise, not a stability guarantee, not a
# deprecation policy. Every kernel crate carries `publish = false`, no
# crates.io release exists and none is planned. Nothing outside this repository
# depends on these names, and this file confers no standing on them: a baseline
# line is a record of today, not an undertaking about tomorrow.
#
# THE TOOL. `cargo-public-api`, pinned below. Rustdoc's JSON output is unstable
# and its schema moves between nightlies; absorbing that churn is the tool's
# entire job, and hand-rolling a dump here would be the bespoke abstraction
# this project refuses when a standard already covers the need. The nightly is
# pinned too — an unpinned one turns a compiler update into a surface change.
#
# Blanket implementations are omitted (`-s`). They come from the standard
# library — `impl<T> Any for T` and friends — and say nothing about this code.
# Auto-trait implementations are KEPT: a type quietly losing `Send` because a
# private field changed is precisely the silent surface change this guard is
# for. Auto-derived implementations are kept too; a `#[derive]` is a decision.
#
# Usage:
#   ci/check-public-api.sh                  check every baseline (the CI mode)
#   ci/check-public-api.sh --bless          rewrite every baseline from the code
#   ci/check-public-api.sh --print-toolchain     the pinned nightly, for CI
#   ci/check-public-api.sh --print-tool-version  the pinned tool, for CI
set -euo pipefail

# The pinned nightly. Read by the CI job through `--print-toolchain`, so the
# workflow never repeats it: a version written twice is a version that drifts,
# which is the defect ci/msrv.sh exists to prevent for the MSRV.
TOOLCHAIN="nightly-2026-08-13"

# The pinned tool, as a `cargo install --version` requirement and as the
# `major.minor` the installed binary must report. Output format changes between
# minor versions, so a mismatch would diff the tool, not the surface.
TOOL_VERSION_REQ="^0.52"
TOOL_VERSION_PREFIX="0.52."

# Each target is `<baseline stem>|<package>|<extra cargo flags>`.
#
# `kernel` appears twice on purpose. The first line is the surface a production
# build sees. The second adds `testing`, which exposes `__register_hook` — the
# substitution hook `kernel-testkit` is built on. Baselining only the union
# would let an item slip from unconditional to feature-gated, or the reverse,
# with no diff at all; baselining only the default set would leave the hook's
# own surface unwatched.
TARGETS=(
  "kernel-core|kernel-core|"
  "kernel|kernel|"
  "kernel-all-features|kernel|--all-features"
  "kernel-macros|kernel-macros|"
  "kernel-testkit|kernel-testkit|"
)

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
baseline_dir="$root/api"

mode="check"
case "${1-}" in
  "") ;;
  --bless|--update) mode="bless" ;;
  --print-toolchain) printf '%s\n' "$TOOLCHAIN"; exit 0 ;;
  --print-tool-version) printf '%s\n' "$TOOL_VERSION_REQ"; exit 0 ;;
  *)
    echo "public-api guard: unknown argument '$1'" >&2
    echo "public-api guard: usage: $0 [--bless|--print-toolchain|--print-tool-version]" >&2
    exit 2
    ;;
esac

fatal() {
  printf 'public-api guard: %s\n' "$1" >&2
  exit 1
}

# --- The tool and the toolchain must both be the pinned ones. ---------------
#
# Neither is installed for the caller. A guard that silently downloads a
# toolchain is a guard that surprises a developer with a gigabyte; it says what
# to run instead, and the CI job runs it explicitly.
command -v cargo-public-api >/dev/null || fatal \
  "cargo-public-api is not installed. Run: cargo install cargo-public-api --locked --version '$TOOL_VERSION_REQ'"

installed_tool="$(cargo public-api --version | awk '{print $2}')"
case "$installed_tool" in
  "$TOOL_VERSION_PREFIX"*) ;;
  *) fatal "cargo-public-api $installed_tool is installed, the baselines were written by ${TOOL_VERSION_PREFIX}x. Run: cargo install cargo-public-api --locked --version '$TOOL_VERSION_REQ'" ;;
esac

rustup toolchain list | grep -q "^$TOOLCHAIN" || fatal \
  "the pinned toolchain is missing. Run: rustup toolchain install $TOOLCHAIN --profile minimal --no-self-update"

mkdir -p "$baseline_dir"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

failures=0
generated=0

for target in "${TARGETS[@]}"; do
  IFS='|' read -r stem package flags <<< "$target"
  baseline="$baseline_dir/$stem.txt"
  current="$work/$stem.txt"

  # `read -a` so an empty flag string expands to no argument at all.
  read -r -a extra <<< "$flags"

  build_log="$work/$stem.log"
  # `--document-hidden-items` is load-bearing, not thoroughness. Every
  # feature-gated item this crate has is `#[doc(hidden)]` — the substitution
  # hook and the four `Registry` replacements the testkit reaches — and
  # cargo-public-api omits hidden items by default. Without this the five
  # things the `testing` feature exists to gate are the only five the guard
  # cannot see, which an audit proved by adding a parameter to one of them and
  # watching the guard pass.
  if ! RUSTUP_TOOLCHAIN="$TOOLCHAIN" RUSTDOCFLAGS="--document-hidden-items" cargo public-api \
        --package "$package" "${extra[@]}" \
        --simplified --color never >"$current" 2>"$build_log"; then
    printf 'public-api guard: cargo-public-api failed on %s\n' "$stem" >&2
    sed 's/^/    /' "$build_log" >&2
    failures=$((failures + 1))
    continue
  fi

  if [ "$mode" = "bless" ]; then
    cp "$current" "$baseline"
    printf 'public-api guard: wrote %s (%s items)\n' \
      "api/$stem.txt" "$(wc -l <"$current" | tr -d ' ')"
    generated=$((generated + 1))
    continue
  fi

  if [ ! -f "$baseline" ]; then
    printf 'public-api guard: no baseline for %s — api/%s.txt does not exist\n' "$stem" "$stem" >&2
    failures=$((failures + 1))
    continue
  fi

  if ! diff -u --label "api/$stem.txt" --label "$package (current)" \
        "$baseline" "$current" >"$work/$stem.diff"; then
    printf 'public-api guard: the public surface of %s no longer matches api/%s.txt\n' "$package" "$stem" >&2
    cat "$work/$stem.diff" >&2
    failures=$((failures + 1))
  fi
  generated=$((generated + 1))
done

if [ "$mode" = "bless" ]; then
  echo "public-api guard: $generated baseline(s) rewritten — review the diff before committing"
  exit 0
fi

if [ "$failures" -ne 0 ]; then
  echo "public-api guard: $failures baseline(s) differ" >&2
  echo "public-api guard: a line with '+' is an item this change ADDS to the public surface, a line with '-' is one it REMOVES" >&2
  echo "public-api guard: if the change is intended, run ./ci/check-public-api.sh --bless and commit api/ with it, so the surface change is reviewed alongside the code" >&2
  exit 1
fi

echo "public-api guard: ok ($generated baseline(s) match, $(cat "$baseline_dir"/*.txt | wc -l | tr -d ' ') items)"
