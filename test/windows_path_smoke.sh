#!/usr/bin/env bash
set -euo pipefail

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"

if [ ! -x "$DOLTLITE" ]; then
  echo "SKIP: doltlite binary not found ($DOLTLITE)"
  exit 0
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-path-smoke.XXXXXX")"
cleanup() {
  rm -rf "$TMP"
}
trap cleanup EXIT

run_case() {
  local label="$1"
  local db="$2"
  rm -f "$db" "$db-wal" "$db-shm" "$db-lock"

  "$DOLTLITE" "$db" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
     INSERT INTO t VALUES(1,'a');
     SELECT dolt_commit('-A','-m','c1');" >/dev/null
  test "$("$DOLTLITE" "$db" "SELECT v FROM t WHERE id=1;")" = "a"
  test "$("$DOLTLITE" "$db" "SELECT count(*) FROM dolt_log;")" = "2"
  echo "PASS: $label"
}

mkdir -p "$TMP/dir with spaces"

run_case "posix temp path" "$TMP/basic.db"
run_case "path with spaces" "$TMP/dir with spaces/db file.db"

if command -v cygpath >/dev/null 2>&1; then
  run_case "windows-style temp path" "$(cygpath -m "$TMP/windows-style.db")"
fi
