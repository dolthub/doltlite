#!/bin/bash
#
# Oracle tests for table-scoped VC operations and schema-change interplay
# across PRIMARY KEY shapes: selective staging (dolt_add of one table),
# table-level checkout restore, diff_stat, and data diffs/history that span
# an ALTER TABLE ADD COLUMN on a clustered table. These surfaces had oracle
# coverage only for `id INT PRIMARY KEY` tables.

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

translate_for_dolt() {
  sed -E "
    s/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g
    s/dolt_diff_(stat|summary)([^a-zA-Z0-9_])/@@DOLT_DIFF_\1@@\2/g
    s/dolt_diff_([a-zA-Z0-9_]+)\('([^']*)', *'([^']*)'\)/dolt_diff_\1 WHERE to_commit IN (SELECT commit_hash FROM dolt_log('\2..\3'))/g
    s/@@DOLT_DIFF_(stat|summary)@@/dolt_diff_\1/g
  "
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
    vc_oracle_init_repo
    {
      echo "$dolt_setup"
      echo "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: table-scoped ops across PK shapes ==="
echo ""

for shape in comp_int text_pk pk_only; do
  case $shape in
    comp_int)
      DDL="CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));"
      SEED="INSERT INTO t VALUES (1,1,'r1'), (1,2,'r2'), (2,1,'r3');"
      MUT="INSERT INTO t(a,b,v) VALUES (3,3,'r4'); DELETE FROM t WHERE a = 1 AND b = 2;"
      PROJ="SELECT CONCAT('R|', a, '|', b, '|', v) FROM t ORDER BY a, b;"
      ;;
    text_pk)
      DDL="CREATE TABLE t(k VARCHAR(30), v TEXT, PRIMARY KEY(k));"
      SEED="INSERT INTO t VALUES ('alpha','r1'), ('beta','r2'), ('gamma','r3');"
      MUT="INSERT INTO t(k,v) VALUES ('delta','r4'); DELETE FROM t WHERE k = 'beta';"
      PROJ="SELECT CONCAT('R|', k, '|', v) FROM t ORDER BY k;"
      ;;
    pk_only)
      DDL="CREATE TABLE t(p INTEGER, c INTEGER, PRIMARY KEY(p, c));"
      SEED="INSERT INTO t VALUES (1,1), (1,2), (2,1);"
      MUT="INSERT INTO t(p,c) VALUES (3,3); DELETE FROM t WHERE p = 1 AND c = 2;"
      PROJ="SELECT CONCAT('R|', p, '|', c) FROM t ORDER BY p, c;"
      ;;
  esac

  echo "--- shape: $shape ---"

  # Two tables so table-scoped operations distinguish their target.
  BASE="
$DDL
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
$SEED
INSERT INTO u VALUES (1,'u1');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
"

  # Selective staging: only t is staged and committed; u stays dirty.
  oracle "${shape}_add_selective" "
$BASE
$MUT
INSERT INTO u VALUES (2,'u2');
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'just t');
" "SELECT CONCAT('R|', table_name, '|', staged, '|', status) FROM dolt_status;"

  # Table-level checkout: discard t's working changes only.
  oracle "${shape}_checkout_table" "
$BASE
$MUT
INSERT INTO u VALUES (2,'u2');
SELECT dolt_checkout('t');
" "$PROJ"

  oracle "${shape}_checkout_table_status" "
$BASE
$MUT
INSERT INTO u VALUES (2,'u2');
SELECT dolt_checkout('t');
" "SELECT CONCAT('R|', table_name, '|', staged, '|', status) FROM dolt_status;"

  # diff_stat over a committed change.
  oracle "${shape}_diff_stat" "
$BASE
$MUT
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
" "SELECT CONCAT('R|', table_name, '|', rows_added, '|', rows_deleted, '|', rows_modified) FROM dolt_diff_stat('HEAD~1', 'HEAD');"

  # Schema change on a clustered table: rows written before and after an
  # ADD COLUMN, then data diff and history spanning the change.
  SCHEMA_SETUP="
$BASE
ALTER TABLE t ADD COLUMN extra TEXT;
$MUT
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'wider');
"

  oracle "${shape}_schema_change_state" "$SCHEMA_SETUP" \
    "SELECT CONCAT('R|', count(*), '|', count(extra)) FROM t;"

  oracle "${shape}_schema_change_history" "$SCHEMA_SETUP" \
    "SELECT CONCAT('R|', count(*)) FROM dolt_history_t;"

  oracle "${shape}_schema_change_diff" "$SCHEMA_SETUP" \
    "SELECT CONCAT('R|', diff_type, '|', count(*)) FROM dolt_diff_t('HEAD~1','HEAD') GROUP BY diff_type;"
done

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ -n "$FAILED_NAMES" ]; then
  echo "Failed:$FAILED_NAMES"
fi
[ "$fail" -eq 0 ]
