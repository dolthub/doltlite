#!/bin/bash

set -u

SRCDIR="${1:-src}"
ROOT="$(cd "$SRCDIR/.." && pwd)"
TMPFILE=$(mktemp)
trap "rm -f $TMPFILE" EXIT

cd "$ROOT" || exit 2
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

raw_matches \
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
