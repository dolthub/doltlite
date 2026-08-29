#!/bin/bash

set -u

SRCDIR="${1:-src}"
ROOT="$(cd "$SRCDIR/.." && pwd)"
TMPFILE=$(mktemp)
trap "rm -f $TMPFILE" EXIT

cd "$ROOT" || exit 2

if ! command -v rg >/dev/null 2>&1; then
  echo "lint_no_raw_os_fileio: ripgrep (rg) is required but not found" >&2
  echo "  install ripgrep or run this lint on a host that has it; refusing to" >&2
  echo "  pass vacuously without the tool that performs the scan." >&2
  exit 2
fi

shopt -s nullglob

FILES=(
  src/chunk_*.c src/chunk_*.h
  src/prolly_*.c src/prolly_*.h
  src/doltlite*.c src/doltlite*.h
  src/sortkey.c src/sortkey.h
  src/pager_shim.c
  src/os_kv.c
)

CALL_RE='\b(open|close|read|write|pread|pwrite|lseek|stat|fstat|access|unlink|rename|mkdir|rmdir|remove|fopen|fclose|fread|fwrite|fflush|fsync|fdatasync|flock|fcntl|mmap|munmap)[[:space:]]*\('
INCLUDE_RE='#[[:space:]]*include[[:space:]]*<((sys/)?stat|fcntl|unistd|sys/file|sys/mman|dirent)\.h>'

raw_matches() {
  rg -n "$CALL_RE" "${FILES[@]}" 2>/dev/null || true
  rg -n "$INCLUDE_RE" "${FILES[@]}" 2>/dev/null || true
}

# creds: OS sidecar + private-file modes. gc/commit: unistd.h for crash-test _exit().
# net.h: sockets; fcntl only for O_NONBLOCK.
# chunk_store_lock.c gen sidecar: the cross-process change counter needs a
# writable shared mapping, which the VFS cannot express (xFetch is
# read-only). The -gen sidecar is never byte-range locked, so its raw
# open/close cannot drop POSIX locks the process holds elsewhere.
raw_matches \
  | grep -Ev '^src/chunk_store_lock\.c:[0-9]+:.*\b(open|close|mmap|munmap|ftruncate)[[:space:]]*\(' \
  | grep -Ev '^src/chunk_store_lock\.c:[0-9]+:#[[:space:]]*include <(sys/mman|fcntl|unistd)\.h>' \
  | grep -Ev '^src/doltlite_creds\.c:' \
  | grep -Ev '^src/(doltlite_gc|chunk_store_commit)\.c:[0-9]+:#include <unistd\.h>' \
  | grep -Ev '^src/doltlite_net\.h:[0-9]+:#include <(unistd|fcntl)\.h>' \
  | grep -Ev '^src/doltlite_net\.h:[0-9]+:.*\bfcntl[[:space:]]*\(' \
  | grep -Ev '^src/doltlite_remotesrv\.c:[0-9]+:.*\b(read|close)[[:space:]]*\(' \
  | grep -Ev '^src/doltlite_http_remote\.c:[0-9]+:.*\b(read|close)[[:space:]]*\(' \
  | grep -Ev '^src/doltlite_remote\.c:[0-9]+:.*\bwrite[[:space:]]*\(' \
  | grep -Ev '^src/os_kv\.c:[0-9]+:.*\b(fopen|fclose|fread|unlink|access|stat)[[:space:]]*\(' \
  | grep -Ev '^src/(doltlite_remotesrv|doltlite_http_remote|doltlite_remote|os_kv)\.c:[0-9]+:#include <unistd\.h>' \
  | grep -Ev '^src/os_kv\.c:[0-9]+:#include <sys/stat\.h>' \
  > "$TMPFILE"

NFAIL=$(wc -l < "$TMPFILE" | tr -d ' ')
if [ "$NFAIL" -eq 0 ]; then
  echo "lint_no_raw_os_fileio: all checks passed"
  exit 0
fi

echo "lint_no_raw_os_fileio: raw OS file IO found in DoltLite-owned code"
cat "$TMPFILE" | sed 's/^/  /'
echo ""
echo "Use sqlite3_vfs/sqlite3Os* APIs for database-file access, or add a narrow"
echo "allowlist entry with a comment explaining why the VFS cannot express it."
exit 1
