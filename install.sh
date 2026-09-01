#!/usr/bin/env bash
# Install the doltlite CLI and doltlite-remotesrv binaries from the latest
# (or a pinned) GitHub release. Pipe me through bash:
#
#   sudo bash -c 'curl -fsSL https://github.com/dolthub/doltlite/releases/latest/download/install.sh | bash'
#
# Honored environment variables:
#   DOLTLITE_INSTALL_DIR — target directory (default /usr/local/bin)
#   DOLTLITE_VERSION     — pin to a specific version, e.g. v0.10.3
#                          (default: the latest GitHub release)
#   DOLTLITE_BASE_URL    — override the release asset URL (tests)

set -euo pipefail

INSTALL_DIR="${DOLTLITE_INSTALL_DIR:-/usr/local/bin}"

OS="$(uname -s)"
case "$OS" in
  Linux)  PLATFORM="linux" ;;
  Darwin) PLATFORM="osx" ;;
  *) echo "doltlite: unsupported OS '$OS' — try 'go install' from source" >&2; exit 1 ;;
esac

ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) PKG_ARCH="x64" ;;
  arm64|aarch64) PKG_ARCH="arm64" ;;
  *) echo "doltlite: unsupported architecture '$ARCH'" >&2; exit 1 ;;
esac

TARGET="${PLATFORM}-${PKG_ARCH}"

# The release matrix builds linux-x64, linux-arm64, osx-arm64, osx-x64, and
# win-x64. Other combinations don't have release artifacts; fall back to
# building from source.
case "$TARGET" in
  linux-x64|linux-arm64|osx-arm64|osx-x64) ;;
  *)
    echo "doltlite: no prebuilt binary for ${TARGET}." >&2
    echo "Build from source — see https://github.com/dolthub/doltlite#building" >&2
    exit 1
    ;;
esac

# Resolve the version: pinned via env, or latest from GitHub.
if [ -n "${DOLTLITE_VERSION:-}" ]; then
  VERSION="$DOLTLITE_VERSION"
else
  VERSION="$(curl -fsSL https://api.github.com/repos/dolthub/doltlite/releases/latest \
    | grep '"tag_name"' \
    | head -1 \
    | sed -E 's/.*"tag_name": "([^"]+)".*/\1/')"
fi
if [ -z "$VERSION" ]; then
  echo "doltlite: failed to determine latest version" >&2
  exit 1
fi
VERSION_NUM="${VERSION#v}"

# INSTALL_DIR is the bin destination (default /usr/local/bin); PREFIX is
# its parent — that's where include/ and lib/ land. Mirrors `make install`.
PREFIX="$(dirname "${INSTALL_DIR}")"
INCLUDE_DIR="${PREFIX}/include"
LIB_DIR="${PREFIX}/lib"

# Fail fast with a helpful message if the user forgot sudo. /usr/local is
# root-owned on most systems.
can_write() {
  local d="$1"
  # Walk up to the first existing ancestor and check whether we could
  # create/modify under it.
  while [ ! -d "$d" ]; do
    d="$(dirname "$d")"
  done
  [ -w "$d" ]
}
if [ "$(id -u)" -ne 0 ]; then
  for d in "$INSTALL_DIR" "$INCLUDE_DIR" "$LIB_DIR"; do
    if ! can_write "$d"; then
      cat >&2 <<EOF
doltlite: ${d} is not writable as $(id -un). Re-run with sudo:

  sudo bash -c 'curl -fsSL https://github.com/dolthub/doltlite/releases/latest/download/install.sh | bash'

Or set DOLTLITE_INSTALL_DIR to a directory you own (e.g. \$HOME/.local/bin).
EOF
      exit 1
    fi
  done
fi

# Pick the shared-library extension per platform.
case "$PLATFORM" in
  linux) SHARED_EXT=".so" ;;
  osx)   SHARED_EXT=".dylib" ;;
esac

TOOLS_ZIP="doltlite-tools-${TARGET}-${VERSION_NUM}.zip"
LIB_ZIP="doltlite-lib-${TARGET}-${VERSION_NUM}.zip"
BASE_URL="${DOLTLITE_BASE_URL:-https://github.com/dolthub/doltlite/releases/download/${VERSION}}"

TMP_DIR="$(mktemp -d -t doltlite-install-XXXXXX)"
trap 'rm -rf "$TMP_DIR"' EXIT

fetch() {
  local name="$1"
  echo "Downloading ${BASE_URL}/${name}..."
  curl -fsSL -o "${TMP_DIR}/${name}" "${BASE_URL}/${name}"
  unzip -q -d "${TMP_DIR}" "${TMP_DIR}/${name}"
}

fetch "${TOOLS_ZIP}"
fetch "${LIB_ZIP}"

TOOLS_DIR="${TMP_DIR}/doltlite-tools-${TARGET}-${VERSION_NUM}"
LIB_SRC_DIR="${TMP_DIR}/doltlite-lib-${TARGET}-${VERSION_NUM}"

mkdir -p "${INSTALL_DIR}" "${INCLUDE_DIR}" "${LIB_DIR}"

for bin in doltlite doltlite-remotesrv; do
  src="${TOOLS_DIR}/${bin}"
  [ -f "$src" ] || continue
  install -m 0755 "$src" "${INSTALL_DIR}/${bin}"
  echo "Installed ${INSTALL_DIR}/${bin}"
done

# Deb and Homebrew install doltlite.h only. sqlite3.h collides with
# system SQLite headers; skip it even if a lib zip still contains it.
for hdr in doltlite.h doltlite_remotesrv.h; do
  if [ -f "${LIB_SRC_DIR}/${hdr}" ]; then
    install -m 0644 "${LIB_SRC_DIR}/${hdr}" "${INCLUDE_DIR}/${hdr}"
    echo "Installed ${INCLUDE_DIR}/${hdr}"
  fi
done
if [ -f "${LIB_SRC_DIR}/libdoltlite.a" ]; then
  install -m 0644 "${LIB_SRC_DIR}/libdoltlite.a" "${LIB_DIR}/libdoltlite.a"
  echo "Installed ${LIB_DIR}/libdoltlite.a"
fi
if [ -f "${LIB_SRC_DIR}/libdoltlite${SHARED_EXT}" ]; then
  install -m 0755 "${LIB_SRC_DIR}/libdoltlite${SHARED_EXT}" "${LIB_DIR}/libdoltlite${SHARED_EXT}"
  echo "Installed ${LIB_DIR}/libdoltlite${SHARED_EXT}"
fi

cat <<EOF

doltlite ${VERSION_NUM} installed.

Verify with:
  doltlite :memory: "SELECT dolt_version();"

If ${INSTALL_DIR} is not on your \$PATH, add it or set
DOLTLITE_INSTALL_DIR to a directory that is.
EOF
