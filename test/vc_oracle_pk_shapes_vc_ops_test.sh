#!/bin/bash
# Row-addressing VC ops once per PRIMARY KEY shape vs Dolt. Single-shape suites missed clustered keys.

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

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
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  dolt_query=$(vc_oracle_translate_for_dolt "$query")

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

echo "=== Version Control Oracle Tests: VC ops across PK shapes ==="
echo ""

# Shapes without a value column substitute delete+insert for the r1 UPDATE.
for shape in comp_int text_pk mixed pk_only; do
  case $shape in
    comp_int)
      DDL="CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));"
      SEED="INSERT INTO t VALUES (1,1,'r1'), (1,2,'r2'), (2,1,'r3');"
      ROW4="INSERT INTO t VALUES (3,3,'r4');"
      ROW5="INSERT INTO t VALUES (5,5,'r5');"
      EDIT_R1="UPDATE t SET v = 'r1x' WHERE a = 1 AND b = 1;"
      DEL_R2="DELETE FROM t WHERE a = 1 AND b = 2;"
      PROJ="SELECT CONCAT('R|', a, '|', b, '|', v) FROM t ORDER BY a, b;"
      ;;
    text_pk)
      DDL="CREATE TABLE t(k VARCHAR(30), v TEXT, PRIMARY KEY(k));"
      SEED="INSERT INTO t VALUES ('alpha','r1'), ('beta','r2'), ('gamma','r3');"
      ROW4="INSERT INTO t VALUES ('delta','r4');"
      ROW5="INSERT INTO t VALUES ('epsilon','r5');"
      EDIT_R1="UPDATE t SET v = 'r1x' WHERE k = 'alpha';"
      DEL_R2="DELETE FROM t WHERE k = 'beta';"
      PROJ="SELECT CONCAT('R|', k, '|', v) FROM t ORDER BY k;"
      ;;
    mixed)
      DDL="CREATE TABLE t(x VARCHAR(20), y INTEGER, v TEXT, PRIMARY KEY(x, y));"
      SEED="INSERT INTO t VALUES ('a',1,'r1'), ('a',2,'r2'), ('b',1,'r3');"
      ROW4="INSERT INTO t VALUES ('c',3,'r4');"
      ROW5="INSERT INTO t VALUES ('e',5,'r5');"
      EDIT_R1="UPDATE t SET v = 'r1x' WHERE x = 'a' AND y = 1;"
      DEL_R2="DELETE FROM t WHERE x = 'a' AND y = 2;"
      PROJ="SELECT CONCAT('R|', x, '|', y, '|', v) FROM t ORDER BY x, y;"
      ;;
    pk_only)
      DDL="CREATE TABLE t(p INTEGER, c INTEGER, PRIMARY KEY(p, c));"
      SEED="INSERT INTO t VALUES (1,1), (1,2), (2,1);"
      ROW4="INSERT INTO t VALUES (3,3);"
      ROW5="INSERT INTO t VALUES (5,5);"
      EDIT_R1="DELETE FROM t WHERE p = 1 AND c = 1; INSERT INTO t VALUES (1,9);"
      DEL_R2="DELETE FROM t WHERE p = 1 AND c = 2;"
      PROJ="SELECT CONCAT('R|', p, '|', c) FROM t ORDER BY p, c;"
      ;;
  esac

  echo "--- shape: $shape ---"

  BASE="
$DDL
$SEED
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
"

  oracle "${shape}_merge" "
$BASE
SELECT dolt_checkout('-b', 'feat');
$ROW4
$EDIT_R1
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
$DEL_R2
$ROW5
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feat');
" "$PROJ"

  oracle "${shape}_cherry_pick" "
$BASE
SELECT dolt_checkout('-b', 'feat');
$ROW4
$DEL_R2
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'cherry');
SELECT dolt_checkout('main');
$ROW5
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main2');
SELECT dolt_cherry_pick('feat');
" "$PROJ"

  oracle "${shape}_revert" "
$BASE
$EDIT_R1
$ROW4
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
SELECT dolt_revert('HEAD');
" "$PROJ"

  oracle "${shape}_rebase" "
$BASE
SELECT dolt_checkout('-b', 'feat');
$ROW4
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
$EDIT_R1
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f2');
SELECT dolt_checkout('main');
$ROW5
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
" "$PROJ"

  oracle "${shape}_reset_hard" "
$BASE
$EDIT_R1
$DEL_R2
$ROW4
SELECT dolt_reset('--hard');
" "$PROJ"

  oracle "${shape}_checkout" "
$BASE
SELECT dolt_checkout('-b', 'feat');
$ROW4
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
$ROW5
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feat');
" "$PROJ"

  oracle "${shape}_status" "
$BASE
$EDIT_R1
$ROW4
" "SELECT CONCAT('R|', table_name, '|', staged, '|', status) FROM dolt_status;"
done

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ -n "$FAILED_NAMES" ]; then
  echo "Failed:$FAILED_NAMES"
fi
[ "$fail" -eq 0 ]
