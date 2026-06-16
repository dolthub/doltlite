#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

dl() {
  local db="$1" sql="$2" tag="$3"
  "$DOLTLITE" "$db" "$sql" 2>"$TMPROOT/$tag.err"
}

dl_setup() {
  local db="$1" tag="$2"
  "$DOLTLITE" "$db" >"$TMPROOT/$tag.out" 2>"$TMPROOT/$tag.err"
}

pass_name() {
  pass=$((pass+1))
  echo "  PASS: $1"
}

fail_name() {
  fail=$((fail+1))
  FAILED_NAMES="$FAILED_NAMES $1"
  echo "  FAIL: $1"
}

expect_eq() {
  local name="$1" want="$2" got="$3"
  if [ "$want" = "$got" ]; then
    pass_name "$name"
  else
    fail_name "$name"
    echo "    want: |$want|"
    echo "    got:  |$got|"
  fi
}

expect_error_match() {
  local name="$1" db="$2" sql="$3" pat="$4" tag="$5"
  local out
  out=$("$DOLTLITE" "$db" "$sql" 2>"$TMPROOT/$tag.err" || true)
  if printf '%s\n' "$out" && cat "$TMPROOT/$tag.err" | grep -qiE "$pat"; then
    pass_name "$name"
  elif printf '%s\n%s\n' "$out" "$(cat "$TMPROOT/$tag.err")" | grep -qiE "$pat"; then
    pass_name "$name"
  else
    fail_name "$name"
    echo "    wanted pattern: $pat"
    echo "    stdout: |$out|"
    echo "    stderr: |$(cat "$TMPROOT/$tag.err")|"
  fi
}

run_tx_oracle_case() {
  local name="$1" dl_setup_sql="$2" dl_tx_sql="$3" dolt_tx_sql="${4:-$3}" dolt_setup_sql="${5:-$2}"
  local dl_db="$TMPROOT/${name}_dl.db"
  local dt_dir="$TMPROOT/${name}_dt"
  rm -f "$dl_db"
  rm -rf "$dt_dir"
  mkdir -p "$dt_dir"

  printf '%s\n' "$dl_setup_sql" | dl_setup "$dl_db" "${name}_dl_setup"
  local dl_out
  dl_out=$(printf '%s\n' "$dl_tx_sql" | "$DOLTLITE" "$dl_db" 2>"$TMPROOT/${name}_dl.err" | grep -E '^[0-9]+\|[0-9]+\|' | tail -n 1 | tr -d '\r')

  (
    cd "$dt_dir" || exit 1
    "$DOLT" init >/dev/null 2>&1
    "$DOLT" sql <<SQL >/dev/null 2>"$TMPROOT/${name}_dt_setup.err"
$dolt_setup_sql
SQL
    "$DOLT" sql -r csv -q "$dolt_tx_sql" 2>"$TMPROOT/${name}_dt.err"
  ) > "$TMPROOT/${name}_dt.out"
  local dt_out
  dt_out=$(tr -d '"' < "$TMPROOT/${name}_dt.out" | grep -E '^[0-9]+\|[0-9]+\|' | tail -n 1 | tr -d '\r')

  expect_eq "${name}_tx_matches_dolt" "$dt_out" "$dl_out"
}

run_tx_expected_case() {
  local name="$1" setup_sql="$2" tx_sql="$3" expected="$4"
  local db="$TMPROOT/${name}.db"
  rm -f "$db"

  printf '%s\n' "$setup_sql" | dl_setup "$db" "${name}_setup"
  local out
  out=$(printf '%s\n' "$tx_sql" | "$DOLTLITE" "$db" 2>"$TMPROOT/${name}.err" | grep -E '^[0-9]+\|[0-9]+\|' | tail -n 1 | tr -d '\r')
  expect_eq "${name}_expected_state" "$expected" "$out"
}

run_success_expected_case() {
  local name="$1" setup_sql="$2" query="$3" expected="$4" op="${5:-merge}"
  local db="$TMPROOT/${name}.db"
  rm -f "$db"

  printf '%s\n' "$setup_sql" | dl_setup "$db" "${name}_setup"
  local out
  out=$("$DOLTLITE" "$db" "$query" 2>"$TMPROOT/${name}_query.err" | tr -d '\r')
  expect_eq "${name}_${op}_expected_state" "$expected" "$out"
}

run_success_oracle_case() {
  local name="$1" setup_sql="$2" query="$3" op="${4:-merge}"
  local dl_db="$TMPROOT/${name}_dl.db"
  local dt_dir="$TMPROOT/${name}_dt"
  rm -f "$dl_db"
  rm -rf "$dt_dir"
  mkdir -p "$dt_dir"

  printf '%s\n' "$setup_sql" | dl_setup "$dl_db" "${name}_dl_setup"
  local dl_out
  dl_out=$("$DOLTLITE" "$dl_db" "$query" 2>"$TMPROOT/${name}_dl_query.err" | tr -d '\r')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup_sql")
  (
    cd "$dt_dir" || exit 1
    vc_oracle_init_repo
    printf '%s\n' "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$TMPROOT/${name}_dt_setup.err"
    "$DOLT" sql -r csv -q "$query" 2>"$TMPROOT/${name}_dt_query.err"
  ) > "$TMPROOT/${name}_dt.out"
  local dt_out
  dt_out=$(tail -n 1 "$TMPROOT/${name}_dt.out" | tr -d '"\r')

  expect_eq "${name}_${op}_matches_dolt" "$dt_out" "$dl_out"
}

echo "=== Version Control Oracle Tests: merge/cherry-pick constraints ==="
echo ""

STATE_COUNTS="SELECT concat(
  (SELECT count(*) FROM p), '|',
  (SELECT count(*) FROM c), '|',
  (SELECT group_concat(concat(id, ':', u) order by id) FROM p), '|',
  (SELECT group_concat(concat(id, ':', u) order by id) FROM c));"

echo "--- success: feature adds FK table family with non-PK parent key ---"
run_success_oracle_case \
  "merge_add_fk_family_unique_parent" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES (1,100);
INSERT INTO c VALUES (1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t2(id INTEGER PRIMARY KEY, v INT CHECK(v>0));
INSERT INTO t2 SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t2 RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_check');
SELECT dolt_merge('feat');" \
  "$STATE_COUNTS" \
  "merge"
echo ""

echo "--- success: cherry-pick adds FK table family with non-PK parent key ---"
run_success_oracle_case \
  "cherry_pick_add_fk_family_unique_parent" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES (1,100);
INSERT INTO c VALUES (1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t2(id INTEGER PRIMARY KEY, v INT CHECK(v>0));
INSERT INTO t2 SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t2 RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_check');
SELECT dolt_cherry_pick('feat');" \
  "$STATE_COUNTS" \
  "cherry_pick"
echo ""

echo "--- success: parent table changes only ---"
run_success_oracle_case \
  "merge_fk_parent_changed_only" \
"CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE, label TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES (1,100,'base');
INSERT INTO c VALUES (1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE p SET label='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_parent');
SELECT dolt_checkout('main');
CREATE TABLE marker(id INTEGER PRIMARY KEY, v INT);
INSERT INTO marker VALUES (1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_marker');
SELECT dolt_merge('feat');" \
  "SELECT concat((SELECT label FROM p WHERE id=1), '|',
    (SELECT count(*) FROM c WHERE u=100));" \
  "merge"
echo ""

echo "--- success: child table changes only ---"
run_success_oracle_case \
  "merge_fk_child_changed_only" \
"CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES (1,100),(2,200);
INSERT INTO c VALUES (1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES (2,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_child');
SELECT dolt_checkout('main');
CREATE TABLE marker(id INTEGER PRIMARY KEY, v INT);
INSERT INTO marker VALUES (1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_marker');
SELECT dolt_merge('feat');" \
  "$STATE_COUNTS" \
  "merge"
echo ""

echo "--- success: parent and child both change ---"
run_success_oracle_case \
  "merge_fk_parent_and_child_changed" \
"CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES (1,100);
INSERT INTO c VALUES (1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO p VALUES (3,300);
INSERT INTO c VALUES (3,300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_parent_child');
SELECT dolt_checkout('main');
INSERT INTO p VALUES (2,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_parent');
SELECT dolt_merge('feat');" \
  "$STATE_COUNTS" \
  "merge"
echo ""

echo "--- success: CHECK introduced while valid row arrives ---"
run_success_expected_case \
  "merge_check_introduced_valid_row" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_row');
SELECT dolt_checkout('main');
CREATE TABLE t2(id INTEGER PRIMARY KEY, v INT CHECK(v>0));
INSERT INTO t2 SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t2 RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_check');
SELECT dolt_merge('feat');" \
  "SELECT group_concat(concat(id, ':', v) order by id) FROM t;" \
  "1:10,2:20" \
  "merge"
echo ""

echo "--- success: UNIQUE introduced while non-duplicate row arrives ---"
run_success_expected_case \
  "merge_unique_introduced_valid_row" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, u INT);
INSERT INTO t VALUES (1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_row');
SELECT dolt_checkout('main');
CREATE TABLE t2(id INTEGER PRIMARY KEY, u INT UNIQUE);
INSERT INTO t2 SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t2 RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_unique');
SELECT dolt_merge('feat');" \
  "SELECT group_concat(concat(id, ':', u) order by id) FROM t;" \
  "1:10,2:20" \
  "merge"
echo ""

echo "--- success: FK introduced while valid child row arrives ---"
run_success_expected_case \
  "merge_fk_introduced_valid_child" \
"CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT);
INSERT INTO p VALUES (1,100),(2,200);
INSERT INTO c VALUES (1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES (2,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_child');
SELECT dolt_checkout('main');
CREATE TABLE c2(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO c2 SELECT * FROM c;
DROP TABLE c;
ALTER TABLE c2 RENAME TO c;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_fk');
SELECT dolt_merge('feat');" \
  "$STATE_COUNTS" \
  "2|2|1:100,2:200|1:100,2:200" \
  "merge"
echo ""

echo "--- success: text primary keys ---"
run_success_oracle_case \
  "merge_text_pk_fk" \
"CREATE TABLE p(id VARCHAR(16) PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id VARCHAR(16) PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES ('p1',100);
INSERT INTO c VALUES ('c1',100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO p VALUES ('p2',200);
INSERT INTO c VALUES ('c2',200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
CREATE TABLE marker(id INTEGER PRIMARY KEY, v INT);
INSERT INTO marker VALUES (1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_marker');
SELECT dolt_merge('feat');" \
  "SELECT concat(
    (SELECT count(*) FROM p), '|',
    (SELECT count(*) FROM c), '|',
    (SELECT group_concat(concat(id, ':', u) order by id) FROM p), '|',
    (SELECT group_concat(concat(id, ':', u) order by id) FROM c));" \
  "merge"
echo ""

echo "--- success: composite primary keys ---"
run_success_oracle_case \
  "merge_composite_pk_fk" \
"CREATE TABLE p(a INT, b INT, u INT UNIQUE, PRIMARY KEY(a,b));
CREATE TABLE c(a INT, b INT, u INT, PRIMARY KEY(a,b), FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES (1,1,100);
INSERT INTO c VALUES (1,1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO p VALUES (2,2,200);
INSERT INTO c VALUES (2,2,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
CREATE TABLE marker(id INTEGER PRIMARY KEY, v INT);
INSERT INTO marker VALUES (1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_marker');
SELECT dolt_merge('feat');" \
  "SELECT concat(
    (SELECT count(*) FROM p), '|',
    (SELECT count(*) FROM c), '|',
    (SELECT group_concat(concat(a, ':', b, ':', u) order by a,b) FROM p), '|',
    (SELECT group_concat(concat(a, ':', b, ':', u) order by a,b) FROM c));" \
  "merge"
echo ""

echo "--- rollback: failed merges restore DoltLite state ---"
echo ""

run_rollback_case() {
  local name="$1" setup_sql="$2" state_query="$3"
  local db="$TMPROOT/$name.db"
  rm -f "$db"

  printf '%s\n' "$setup_sql" | dl_setup "$db" "$name"
  expect_error_match "${name}_merge_errors" "$db" "SELECT dolt_merge('feat');" "constraint violations|rolled back" "${name}_merge"

  local conflicts
  conflicts=$(dl "$db" "SELECT count(*) FROM dolt_conflicts;" "${name}_conflicts")
  expect_eq "${name}_no_conflicts" "0" "$conflicts"

  local violations
  violations=$(dl "$db" "SELECT count(*) FROM dolt_constraint_violations;" "${name}_violations")
  expect_eq "${name}_no_violations" "0" "$violations"

  local state
  state=$(dl "$db" "$state_query" "${name}_state")
  expect_eq "${name}_state_restored" "OK" "$state"
}

echo "--- A. FK orphan rolls back ---"
run_rollback_case \
  "fk_orphan" \
"CREATE TABLE parent(pk INTEGER PRIMARY KEY, v1 INT, UNIQUE(v1));
CREATE TABLE child(pk INTEGER PRIMARY KEY, v1 INT, FOREIGN KEY(v1) REFERENCES parent(v1));
INSERT INTO parent VALUES (1,1),(2,2);
INSERT INTO child VALUES (1,1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (2,2);
SELECT dolt_commit('-Am','feat_add_child');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=2;
SELECT dolt_commit('-Am','main_drop_parent');" \
  "SELECT CASE
      WHEN (SELECT count(*) FROM parent)=1
       AND (SELECT count(*) FROM child WHERE pk=2)=0
       AND (SELECT count(*) FROM child)=1
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- B. UNIQUE merge violation rolls back ---"
run_rollback_case \
  "unique_rows" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1,1,'base1'),(2,2,'base2');
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET u=9, v='feat2' WHERE id=2;
SELECT dolt_commit('-Am','feat_unique');
SELECT dolt_checkout('main');
UPDATE t SET u=9, v='main1' WHERE id=1;
SELECT dolt_commit('-Am','main_unique');" \
  "SELECT CASE
      WHEN (SELECT group_concat(id || ':' || u || ':' || v, ',')
              FROM (SELECT id,u,v FROM t ORDER BY id))='1:9:main1,2:2:base2'
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- C. CHECK merge violation rolls back ---"
run_rollback_case \
  "check_rows" \
"CREATE TABLE t(pk INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1,1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2,-1);
SELECT dolt_commit('-Am','feat_bad');
SELECT dolt_checkout('main');
CREATE TABLE t_new(pk INTEGER PRIMARY KEY, v INT CHECK(v>0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_commit('-Am','main_add_check');" \
  "SELECT CASE
      WHEN (SELECT count(*) FROM t WHERE pk=2)=0
       AND (SELECT count(*) FROM t)=1
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- D. WITHOUT ROWID FK violation rolls back ---"
run_rollback_case \
  "without_rowid_fk" \
"CREATE TABLE parent(pk TEXT PRIMARY KEY, v1 INT, UNIQUE(v1));
CREATE TABLE child(pk TEXT PRIMARY KEY, v1 INT, FOREIGN KEY(v1) REFERENCES parent(v1));
INSERT INTO parent VALUES ('a',1),('b',2);
INSERT INTO child VALUES ('a',1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES ('b',2);
SELECT dolt_commit('-Am','feat_add_child');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk='b';
SELECT dolt_commit('-Am','main_drop_parent');" \
  "SELECT CASE
      WHEN (SELECT count(*) FROM parent)=1
       AND (SELECT count(*) FROM child WHERE pk='b')=0
       AND (SELECT count(*) FROM child)=1
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- E. WITHOUT ROWID UNIQUE violation rolls back ---"
run_rollback_case \
  "without_rowid_unique" \
"CREATE TABLE t(pk TEXT PRIMARY KEY, v1 INT UNIQUE);
INSERT INTO t VALUES ('base',10);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES ('feat',20);
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES ('main',20);
SELECT dolt_commit('-Am','main');" \
  "SELECT CASE
      WHEN (SELECT group_concat(pk || ':' || v1, ',')
              FROM (SELECT pk,v1 FROM t ORDER BY pk))='base:10,main:20'
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- F. WITHOUT ROWID CHECK violation rolls back ---"
run_rollback_case \
  "without_rowid_check" \
"CREATE TABLE t(pk TEXT PRIMARY KEY, v1 INT);
INSERT INTO t VALUES ('a',1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES ('b',-1);
SELECT dolt_commit('-Am','feat_bad');
SELECT dolt_checkout('main');
CREATE TABLE t_new(pk TEXT PRIMARY KEY, v1 INT CHECK(v1>0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_commit('-Am','main_add_check');" \
  "SELECT CASE
      WHEN (SELECT count(*) FROM t WHERE pk='b')=0
       AND (SELECT count(*) FROM t)=1
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- G. Mixed WITHOUT ROWID FK + UNIQUE violation rolls back ---"
run_rollback_case \
  "without_rowid_mixed" \
"CREATE TABLE p(pk TEXT PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(pk TEXT PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES ('p1',1),('p2',2);
INSERT INTO c VALUES ('c1',1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES ('bad',2);
INSERT INTO p VALUES ('dup',3);
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
DELETE FROM p WHERE pk='p2';
UPDATE p SET u=3 WHERE pk='p1';
SELECT dolt_commit('-Am','main');" \
  "SELECT CASE
      WHEN (SELECT count(*) FROM c WHERE pk='bad')=0
       AND (SELECT count(*) FROM p WHERE pk='dup')=0
       AND (SELECT group_concat(pk || ':' || u, ',')
              FROM (SELECT pk,u FROM p ORDER BY pk))='p1:3'
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- H. Explicit transaction keeps merge constraint state live ---"
run_tx_expected_case \
  "tx_unique_persists" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1,1,'base1'),(2,2,'base2');
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET u=9, v='feat2' WHERE id=2;
SELECT dolt_commit('-Am','feat_unique');
SELECT dolt_checkout('main');
UPDATE t SET u=9, v='main1' WHERE id=1;
SELECT dolt_commit('-Am','main_unique');" \
"BEGIN;
SELECT dolt_merge('feat');
SELECT (SELECT count(*) FROM dolt_conflicts) || '|' ||
       (SELECT count(*) FROM dolt_constraint_violations) || '|' ||
       (SELECT group_concat(id || ':' || u || ':' || v, ',')
          FROM (SELECT id,u,v FROM t ORDER BY id));
ROLLBACK;" \
  "0|1|1:9:main1,2:9:feat2"
echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
