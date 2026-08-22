#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0
fail=0
FAILED_NAMES=""

check() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    expected: $expected"
    echo "    actual:   $actual"
  fi
}

objs() {
  "$DOLTLITE" "$1" \
    "SELECT coalesce(group_concat(type || ':' || name), '<none>') FROM sqlite_master;"
}

# Same-session visit of the source used to publish that branch's catalog as
# our working set. The specimen is a drop-versus-edit refusal; the old
# trigger-versus-rename specimen merges now that triggers follow their table.
DB="$TMPROOT/failed_merge.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1');
SELECT dolt_add('-A'), dolt_commit('-m','init');
SELECT dolt_branch('side');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_add('-A'), dolt_commit('-m','ours drop');
SELECT dolt_checkout('side');
UPDATE t SET b='edit' WHERE k=1;
SELECT dolt_add('-A'), dolt_commit('-m','theirs edit');
SELECT dolt_checkout('main');
SELECT dolt_merge('side');
EOF

check "failed_merge_keeps_our_schema" "table:t" "$(objs "$DB")"
check "failed_merge_keeps_our_rows" "1|a1" \
  "$("$DOLTLITE" "$DB" "SELECT k || '|' || a FROM t ORDER BY k;")"
check "failed_merge_keeps_our_columns" "k,a" \
  "$("$DOLTLITE" "$DB" "SELECT group_concat(name) FROM pragma_table_info('t');")"
check "failed_merge_leaves_no_edits" "" \
  "$("$DOLTLITE" "$DB" "SELECT group_concat(table_name || '/' || status) FROM dolt_status;")"
check "failed_merge_keeps_our_head" "ours drop" \
  "$("$DOLTLITE" "$DB" "SELECT message FROM dolt_log LIMIT 1;")"
check "failed_merge_side_untouched" "1|a1|edit" \
  "$("$DOLTLITE" "$DB/side" "SELECT k || '|' || a || '|' || b FROM t;")"

# Same-session checkout used to serialize the catalog of the branch we left.
DB="$TMPROOT/checkout_then_write.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT);
INSERT INTO t VALUES(1,'a1');
SELECT dolt_add('-A'), dolt_commit('-m','init');
SELECT dolt_branch('side');
SELECT dolt_checkout('side');
CREATE TABLE only_on_side(x INTEGER PRIMARY KEY);
SELECT dolt_add('-A'), dolt_commit('-m','side table');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'a2');
EOF

check "write_after_checkout_keeps_our_schema" "table:t" "$(objs "$DB")"
check "write_after_checkout_keeps_rows" "1|a1
2|a2" "$("$DOLTLITE" "$DB" "SELECT k || '|' || a FROM t ORDER BY k;")"

# ROLLBACK after same-session checkout must restore the branch we are on.
DB="$TMPROOT/rollback_after_checkout.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(k INTEGER PRIMARY KEY, a TEXT);
INSERT INTO t VALUES(1,'a1');
SELECT dolt_add('-A'), dolt_commit('-m','init');
SELECT dolt_branch('side');
SELECT dolt_checkout('side');
CREATE TABLE only_on_side(x INTEGER PRIMARY KEY);
SELECT dolt_add('-A'), dolt_commit('-m','side table');
SELECT dolt_checkout('main');
BEGIN;
INSERT INTO t VALUES(3,'a3');
ROLLBACK;
EOF

check "rollback_after_checkout_keeps_our_schema" "table:t" "$(objs "$DB")"
check "rollback_after_checkout_discards_row" "1|a1" \
  "$("$DOLTLITE" "$DB" "SELECT k || '|' || a FROM t ORDER BY k;")"

echo
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -ne 0 ]; then
  echo "failed:$FAILED_NAMES"
  exit 1
fi
