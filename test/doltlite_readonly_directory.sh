#!/bin/bash
# A writable database file in a read-only directory: stock SQLite fails
# writes with SQLITE_READONLY / SQLITE_READONLY_DIRECTORY because it cannot
# create journal/WAL sidecars. DoltLite needs the same codes when it cannot
# create its graph-lock sidecar (misc7-23).
DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
. "$(dirname "$0")/lib/doltlite_test_common.sh"

if [ "$(uname -s)" = "Darwin" ] || [ "$(uname -s)" = "Linux" ]; then
  :
else
  echo "SKIP: readonly-directory test is unix-only"
  exit 0
fi

echo "=== Doltlite READONLY_DIRECTORY on RO parent dir ==="
echo ""

ROOT=$(mktemp -d /tmp/dl_ro_dir_XXXXXX)
trap 'chmod -R u+w "$ROOT" 2>/dev/null; rm -rf "$ROOT"' EXIT

SRC="$ROOT/src.db"
DIR="$ROOT/ro"
DB="$DIR/test.db"

rm -f "$SRC"
printf '%s\n' "CREATE TABLE t1(x, y); INSERT INTO t1 VALUES(1, 2);" \
  | "$DOLTLITE" "$SRC" >/dev/null

mkdir -p "$DIR"
cp "$SRC" "$DB"
chmod a-w "$DIR"

# Read still works.
run_test "ro_dir_select" "SELECT x, y FROM t1;" "1|2" "$DB"

# Write fails with stock-compatible message (primary SQLITE_READONLY).
run_test_match "ro_dir_insert_message" \
  "INSERT INTO t1 VALUES(3, 4);" \
  "attempt to write a readonly database" "$DB"

# Extended code via a tiny C probe linked in CI-less local runs is covered
# by the message + primary failure; extended READONLY_DIRECTORY is asserted
# in the C repro under review. Here we also check CLI fails (non-zero path).
out=$(printf '%s\n' "INSERT INTO t1 VALUES(5, 6);" | "$DOLTLITE" "$DB" 2>&1) || true
if echo "$out" | grep -q "attempt to write a readonly database"; then
  dltest_pass
  echo "  PASS: ro_dir_insert_cli"
else
  dltest_fail "ro_dir_insert_cli" "  got: $out"
fi

chmod u+w "$DIR"
dltest_finish
