#!/bin/bash
#
# Version-control oracle test: dolt_diff_<table>(from_ref, to_ref) TVF form
#
# Dolt exposes row-level range diffs via a generic TVF
# `dolt_diff(from_ref, to_ref, table_name)` whose output schema is
# per-table. SQLite eponymous TVFs declare a static schema at
# connect time, so we expose the same functionality by adding
# (from_ref, to_ref) as positional args to the existing
# `dolt_diff_<table>` virtual table — the table name rides in the
# module name, where it's already per-instance, and the schema is
# the same as the no-arg form.
#
# The oracle query uses the doltlite form:
#
#   SELECT * FROM dolt_diff_users('HEAD~1', 'HEAD')
#
# and a sed transformation rewrites it for Dolt to:
#
#   SELECT * FROM dolt_diff('HEAD~1', 'HEAD', 'users')
#
# Each row is serialized as "R|<pk>|<to_vals>|<from_vals>|<diff_type>"
# so the commit hash columns (which differ between engines —
# doltlite emits resolved hashes, Dolt emits the ref literal) are
# intentionally excluded from the comparison.
#
# Usage: bash vc_oracle_diff_tvf_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Translate setup SQL (SELECT dolt_* → CALL dolt_*) and the query
# (dolt_diff_<t>(...) → dolt_diff(..., '<t>')) for Dolt.
translate_for_dolt() {
  sed -E '
    s/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g
    s/dolt_diff_([a-zA-Z0-9_]+)\(([^)]*)\)/dolt_diff(\2, "\1")/g
  '
}

oracle() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^R|' | sort)

  local dolt_setup dolt_query
  dolt_setup=$(echo "$setup" | translate_for_dolt)
  dolt_query=$(echo "$query" | translate_for_dolt)

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$dolt_query"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

echo "=== Version Control Oracle Tests: dolt_diff TVF form ==="
echo ""

SETUP_LINEAR="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1), (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES (3, 3);
UPDATE t SET v = 20 WHERE id = 2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c3');
"

echo "--- linear history slice ---"

oracle "slice_one_commit" "$SETUP_LINEAR" \
  "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

oracle "slice_two_commits" "$SETUP_LINEAR" \
  "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~2', 'HEAD');"

oracle "slice_full_range" "$SETUP_LINEAR" \
  "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~3', 'HEAD');"

echo "--- ref types ---"

# Named refs: branch names.
oracle "slice_branch_refs" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_c1');
" "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('main', 'feat');"

# Tags.
oracle "slice_tag_refs" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'v1');
SELECT dolt_tag('v1', (SELECT commit_hash FROM dolt_log WHERE message='v1'));
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'v2');
" "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('v1', 'HEAD');"

echo "--- WORKING ref ---"

oracle "slice_to_working" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
INSERT INTO t VALUES (99, 99);
" "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD', 'WORKING');"

echo "--- no diff (same ref both sides) ---"

oracle "slice_no_change" "$SETUP_LINEAR" \
  "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD', 'HEAD');"

echo "--- multi-column table ---"

SETUP_MULTI="
CREATE TABLE m(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));
INSERT INTO m VALUES (1, 1, 'one'), (1, 2, 'two');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
UPDATE m SET v = 'TWO' WHERE a = 1 AND b = 2;
INSERT INTO m VALUES (2, 1, 'three');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
"

oracle "slice_multi_col" "$SETUP_MULTI" \
  "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_m('HEAD~1', 'HEAD');"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
