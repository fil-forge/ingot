#!/usr/bin/env bash
#
# pull-foc-encryption.sh — vendor the pinned foc-encryption reference
# implementation and (re)generate + verify the FEE cross-implementation
# fixtures against it.
#
# The reference is github.com/Kubuxu/foc-encryption-demo, packages/
# foc-encryption, pinned to a fixed commit for reproducibility. The vendored
# source is written under ts/vendor/ (gitignored) and never committed; only this
# script, the ts/ driver, and the generated testdata/ fixtures are.
#
# Usage:
#   ./pull-foc-encryption.sh            # generate the TS fixture + verify all
#   ./pull-foc-encryption.sh verify     # verify committed fixtures only
#   ./pull-foc-encryption.sh generate   # (re)generate the TS fixture only
#
# Requires: bun (https://bun.sh) and either git or curl for the source pull.
set -euo pipefail

REF_REPO="https://github.com/Kubuxu/foc-encryption-demo"
REF_SHA="f0eac6ea1f54bd9100c9d328f39e7509b5bcfdf4"   # pinned for reproducibility
PKG_SUBDIR="packages/foc-encryption"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TS_DIR="$SCRIPT_DIR/ts"
VENDOR="$TS_DIR/vendor/foc-encryption"

# Source files fetched by the raw fallback (the git path copies the whole
# package). This list is fixed for the pinned commit.
SRC_FILES=(
  src/index.ts src/envelope.ts src/blob.ts src/crypto.ts src/kdf.ts
  src/key-utils.ts src/types.ts src/errors.ts
  src/cose/decode.ts src/cose/encode.ts src/cose/headers.ts
  src/cose/structures.ts src/cose/tags.ts
  src/schemes/scheme.ts src/schemes/aes-256-gcm.ts src/schemes/chunked-aes-256-gcm.ts
)

log() { printf '>> %s\n' "$*"; }

# fetch_via_git clones and checks out the pinned SHA, then copies the package
# into the vendor dir. Returns non-zero if git is unavailable or the clone fails
# (e.g. behind a proxy that blocks git), so the caller can fall back to raw.
fetch_via_git() {
  command -v git >/dev/null 2>&1 || return 1
  local tmp
  tmp="$(mktemp -d)"
  if git clone --quiet "$REF_REPO" "$tmp" 2>/dev/null &&
    git -C "$tmp" checkout --quiet "$REF_SHA" 2>/dev/null; then
    rm -rf "$VENDOR"
    mkdir -p "$VENDOR"
    cp -R "$tmp/$PKG_SUBDIR/." "$VENDOR/"
    rm -rf "$tmp"
    return 0
  fi
  rm -rf "$tmp"
  return 1
}

# fetch_via_raw pulls the individual pinned source files over HTTPS. This works
# in environments whose egress allows raw.githubusercontent.com but not git.
fetch_via_raw() {
  command -v curl >/dev/null 2>&1 || {
    echo "error: need git or curl to fetch the reference source" >&2
    exit 1
  }
  local base="https://raw.githubusercontent.com/Kubuxu/foc-encryption-demo/$REF_SHA/$PKG_SUBDIR"
  rm -rf "$VENDOR"
  local f
  for f in "${SRC_FILES[@]}"; do
    mkdir -p "$VENDOR/$(dirname "$f")"
    curl -fsSL "$base/$f" -o "$VENDOR/$f"
  done
}

log "vendoring foc-encryption @ ${REF_SHA:0:12}"
if fetch_via_git; then
  log "source obtained via git clone"
else
  log "git clone unavailable; falling back to raw file fetch"
  fetch_via_raw
  log "source obtained via raw.githubusercontent.com"
fi

command -v bun >/dev/null 2>&1 || {
  echo "error: bun is required to run the TS reference (https://bun.sh)" >&2
  exit 1
}

log "installing harness dependencies (cborg)"
(cd "$TS_DIR" && bun install --silent)

log "running the foc-encryption driver"
(cd "$TS_DIR" && bun driver.ts "${1:-all}")
