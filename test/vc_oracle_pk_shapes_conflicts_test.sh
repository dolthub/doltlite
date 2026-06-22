#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

translate_for_dolt() {
  sed -E '
    s/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g
    s/dolt_diff_(stat|summary)([^a-zA-Z0-9_])/@@DS\1@@\2/g
    s/dolt_diff_([a-zA-Z0-9_]+)\(([^)]*)\)/dolt_diff(\2, "\1")/g
    s/@@DS(stat|summary)@@/dolt_diff_\1/g
  '
}

oracle() {
  local name="$1" setup="$2" query="$3" allow_empty="${4:-}"
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

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  local dt_out
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

oracle_conflict() {
  local name="$1" setup="$2" resolve_and_query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_script
  dl_script=$(printf "%s\n%s\n" "$setup" "$resolve_and_query" \
    | perl -0pe "s/\nSELECT dolt_merge\(/\nBEGIN;\nSELECT dolt_merge\(/")

  local dl_out
  dl_out=$(printf "%s" "$dl_script" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | grep -v '^Merge has' \
           | grep '^R|' \
           | tr -d '\r' | sort)

  local dolt_all
  dolt_all=$(vc_oracle_translate_for_dolt "$(printf '%s\n%s' "$setup" "$resolve_and_query")")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf 'SET @@autocommit = 0;\n'
      printf 'SET @@dolt_allow_commit_conflicts = 1;\n'
      printf '%s\n' "$dolt_all"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^R|' | tr -d '\r' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: varied PK shapes and conflicts ==="
echo ""

echo "--- Group A: REAL primary key ---"

oracle "a_real_pk_diff" "
CREATE TABLE t(pk REAL PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1.5,'a'),(2.25,'b'),(3.75,'c');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');
UPDATE t SET v='B' WHERE pk=2.25;
DELETE FROM t WHERE pk=1.5;
INSERT INTO t VALUES(4.5,'d');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','c2');
" "SELECT CONCAT('R|',IFNULL(to_pk,''),'|',IFNULL(to_v,''),'|',IFNULL(from_pk,''),'|',IFNULL(from_v,''),'|',diff_type)
   FROM dolt_diff_t('HEAD~1','HEAD') ORDER BY IFNULL(to_pk,from_pk);"

oracle "a_real_pk_pk_move" "
CREATE TABLE t(pk REAL PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1.5,'one');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');
UPDATE t SET pk=9.875 WHERE pk=1.5;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','move_pk');
" "SELECT CONCAT('R|',IFNULL(to_pk,''),'|',IFNULL(to_v,''),'|',IFNULL(from_pk,''),'|',IFNULL(from_v,''),'|',diff_type)
   FROM dolt_diff_t('HEAD~1','HEAD') ORDER BY diff_type;"

oracle "a_real_pk_history" "
CREATE TABLE t(pk REAL PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1.5,'x'),(2.25,'y');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','SEED');
UPDATE t SET v='X' WHERE pk=1.5;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','UPDATE1');
INSERT INTO t VALUES(3.75,'z');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','INSERT2');
" "SELECT CONCAT('R|',h.pk,'|',h.v,'|',l.message)
   FROM dolt_history_t h LEFT JOIN dolt_log l ON l.commit_hash=h.commit_hash
   ORDER BY h.pk, l.message;"

oracle "a_real_pk_blame" "
CREATE TABLE t(pk REAL PRIMARY KEY, v INT);
INSERT INTO t VALUES(1.5,10),(2.25,20);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','SEED');
UPDATE t SET v=200 WHERE pk=2.25;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','BUMP');
INSERT INTO t VALUES(3.75,30);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','ADD');
" "SELECT CONCAT('R|',pk,'|',message) FROM dolt_blame_t ORDER BY pk;"

oracle_conflict "a_real_pk_conflict_ours" "
CREATE TABLE t(pk REAL PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1.5,'base'),(2.25,'stable');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');
SELECT dolt_branch('feat');
UPDATE t SET v='main_val' WHERE pk=1.5;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_val' WHERE pk=1.5;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
" "
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--ours','t');
SELECT CONCAT('R|',pk,'|',v) FROM t ORDER BY pk;"

oracle_conflict "a_real_pk_conflict_theirs" "
CREATE TABLE t(pk REAL PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1.5,'base'),(2.25,'stable');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');
SELECT dolt_branch('feat');
UPDATE t SET v='main_val' WHERE pk=1.5;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_val' WHERE pk=1.5;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
" "
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--theirs','t');
SELECT CONCAT('R|',pk,'|',v) FROM t ORDER BY pk;"

echo ""
echo "--- Group B: composite (INT, REAL) primary key ---"

oracle "b_int_real_pk_diff" "
CREATE TABLE t(a INTEGER, b REAL, v TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES(1,1.5,'one'),(1,2.25,'two'),(2,0.75,'three');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');
UPDATE t SET v='TWO' WHERE a=1 AND b=2.25;
INSERT INTO t VALUES(2,1.5,'four');
DELETE FROM t WHERE a=1 AND b=1.5;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','c2');
" "SELECT CONCAT('R|',IFNULL(to_a,''),'|',IFNULL(to_b,''),'|',IFNULL(to_v,''),'|',IFNULL(from_a,''),'|',IFNULL(from_b,''),'|',IFNULL(from_v,''),'|',diff_type)
   FROM dolt_diff_t('HEAD~1','HEAD') ORDER BY IFNULL(to_a,from_a),IFNULL(to_b,from_b);"

oracle "b_int_real_pk_history" "
CREATE TABLE t(a INTEGER, b REAL, v TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES(1,1.5,'x'),(1,2.25,'y');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','SEED');
UPDATE t SET v='Y' WHERE a=1 AND b=2.25;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','UPD');
" "SELECT CONCAT('R|',h.a,'|',h.b,'|',h.v,'|',l.message)
   FROM dolt_history_t h LEFT JOIN dolt_log l ON l.commit_hash=h.commit_hash
   ORDER BY h.a, h.b, l.message;"

oracle_conflict "b_int_real_pk_conflict_ours" "
CREATE TABLE t(a INTEGER, b REAL, v TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES(1,1.5,'base');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');
SELECT dolt_branch('feat');
UPDATE t SET v='main_v' WHERE a=1 AND b=1.5;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_v' WHERE a=1 AND b=1.5;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
" "
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--ours','t');
SELECT CONCAT('R|',a,'|',b,'|',v) FROM t ORDER BY a,b;"

echo ""
echo "--- Group C: composite (TEXT, TEXT) primary key ---"

oracle "c_text_text_pk_diff" "
CREATE TABLE t(cat VARCHAR(32), tag VARCHAR(32), v INT, PRIMARY KEY(cat, tag));
INSERT INTO t VALUES('alice','math',90),('alice','sci',85),('bob','math',75);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');
UPDATE t SET v=95 WHERE cat='alice' AND tag='math';
DELETE FROM t WHERE cat='bob' AND tag='math';
INSERT INTO t VALUES('bob','sci',80);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','c2');
" "SELECT CONCAT('R|',IFNULL(to_cat,''),'|',IFNULL(to_tag,''),'|',IFNULL(to_v,''),'|',IFNULL(from_cat,''),'|',IFNULL(from_tag,''),'|',IFNULL(from_v,''),'|',diff_type)
   FROM dolt_diff_t('HEAD~1','HEAD') ORDER BY diff_type, IFNULL(to_cat,from_cat);"

oracle "c_text_text_pk_blame" "
CREATE TABLE t(cat VARCHAR(32), tag VARCHAR(32), v INT, PRIMARY KEY(cat, tag));
INSERT INTO t VALUES('alice','math',90),('bob','math',75);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','SEED');
UPDATE t SET v=95 WHERE cat='alice' AND tag='math';
SELECT dolt_add('-A'); SELECT dolt_commit('-m','BUMP');
" "SELECT CONCAT('R|',cat,'|',tag,'|',message) FROM dolt_blame_t ORDER BY cat,tag;"

oracle_conflict "c_text_text_pk_conflict_ours" "
CREATE TABLE t(cat VARCHAR(32), tag VARCHAR(32), v INT, PRIMARY KEY(cat, tag));
INSERT INTO t VALUES('alice','math',90);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');
SELECT dolt_branch('feat');
UPDATE t SET v=100 WHERE cat='alice' AND tag='math';
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main');
SELECT dolt_checkout('feat');
UPDATE t SET v=99 WHERE cat='alice' AND tag='math';
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
" "
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--ours','t');
SELECT CONCAT('R|',cat,'|',tag,'|',v) FROM t ORDER BY cat,tag;"

oracle_conflict "c_text_text_pk_conflict_theirs" "
CREATE TABLE t(cat VARCHAR(32), tag VARCHAR(32), v INT, PRIMARY KEY(cat, tag));
INSERT INTO t VALUES('alice','math',90),('bob','sci',70);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');
SELECT dolt_branch('feat');
UPDATE t SET v=100 WHERE cat='alice' AND tag='math';
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main');
SELECT dolt_checkout('feat');
UPDATE t SET v=55 WHERE cat='alice' AND tag='math';
INSERT INTO t VALUES('carol','lang',88);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
" "
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--theirs','t');
SELECT CONCAT('R|',cat,'|',tag,'|',v) FROM t ORDER BY cat,tag;"

echo ""
echo "--- Group D: large and negative integer primary keys ---"

oracle "d_large_int_pk_diff" "
CREATE TABLE t(pk BIGINT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(4294967296,'a'),(8589934592,'b'),(17179869184,'c');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');
UPDATE t SET v='B' WHERE pk=8589934592;
DELETE FROM t WHERE pk=4294967296;
INSERT INTO t VALUES(34359738368,'d');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','c2');
" "SELECT CONCAT('R|',IFNULL(to_pk,''),'|',IFNULL(to_v,''),'|',IFNULL(from_pk,''),'|',IFNULL(from_v,''),'|',diff_type)
   FROM dolt_diff_t('HEAD~1','HEAD') ORDER BY diff_type, IFNULL(to_pk,from_pk);"

oracle "d_negative_int_pk_diff" "
CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(-300,'a'),(-100,'b'),(0,'c'),(50,'d');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','seed');
UPDATE t SET v='NEG' WHERE pk=-300;
INSERT INTO t VALUES(-500,'e');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','c2');
" "SELECT CONCAT('R|',IFNULL(to_pk,''),'|',IFNULL(to_v,''),'|',IFNULL(from_pk,''),'|',IFNULL(from_v,''),'|',diff_type)
   FROM dolt_diff_t('HEAD~1','HEAD') ORDER BY diff_type, IFNULL(to_pk,from_pk);"

oracle "d_mixed_sign_pk_history" "
CREATE TABLE t(pk INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(-1000,1),(-1,2),(0,3),(1,4),(1000,5);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','SEED');
UPDATE t SET v=20 WHERE pk=-1;
UPDATE t SET v=40 WHERE pk=1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','UPD');
" "SELECT CONCAT('R|',h.pk,'|',h.v,'|',l.message)
   FROM dolt_history_t h LEFT JOIN dolt_log l ON l.commit_hash=h.commit_hash
   ORDER BY h.pk, l.message;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failures:$FAILED_NAMES"
  exit 1
fi
