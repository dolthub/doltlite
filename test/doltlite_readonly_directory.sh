#!/bin/bash
# Writable file in a read-only dir: same SQLITE_READONLY as stock when the graph-lock sidecar cannot be created.
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

run_test "ro_dir_select" "SELECT x, y FROM t1;" "1|2" "$DB"

run_test_error_match "ro_dir_insert_message" \
  "INSERT INTO t1 VALUES(3, 4);" \
  "attempt to write a readonly database" "$DB"

# CLI must fail; extended READONLY_DIRECTORY is asserted in the C repro.
out=$(printf '%s\n' "INSERT INTO t1 VALUES(5, 6);" | "$DOLTLITE" "$DB" 2>&1) || true
if echo "$out" | grep -q "attempt to write a readonly database"; then
  dltest_pass
  echo "  PASS: ro_dir_insert_cli"
else
  dltest_fail "ro_dir_insert_cli" "  got: $out"
fi

# A new database there cannot be created: stock refuses at open.
if [ "$(id -u)" = "0" ]; then
  echo "  SKIP: ro_dir_new_database (root ignores directory permissions)"
else
  NEW="$DIR/new.db"
  run_test_error_match "ro_dir_new_database_read_cannot_open" \
    "SELECT 1;" "unable to open database" "$NEW"
  run_test_error_match "ro_dir_new_database_write_cannot_open" \
    "CREATE TABLE x(a);" "unable to open database" "$NEW"
  run_test "ro_dir_new_database_not_created" \
    "SELECT '$(ls -A "$DIR" | tr '\n' ' ')';" "test.db "
  : > "$ROOT/empty.db"
  chmod u+w "$DIR"; mv "$ROOT/empty.db" "$DIR/empty.db"; chmod a-w "$DIR"
  run_test "ro_dir_empty_file_still_opens" "SELECT 1;" "1" "$DIR/empty.db"
fi

chmod u+w "$DIR"
dltest_finish
