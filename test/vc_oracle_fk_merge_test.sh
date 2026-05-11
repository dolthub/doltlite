#!/bin/bash

















set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

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

echo "=== Version Control Oracle Tests: merge constraint rollback ==="
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
