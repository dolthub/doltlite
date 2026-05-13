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

# Helper for cases where the merge MUST succeed (no rollback) and the
# resulting state must match an expected string. Setup must commit the
# pre-merge state on both branches; this helper performs the merge as a
# separate statement so we can detect spurious errors. The state query
# should produce a single line ending in 'OK' on success and 'BAD' on
# unexpected state — matches the style of run_rollback_case.
#
# Setup scripts that need cascading FK behavior must start with
# 'PRAGMA foreign_keys=1;' since SQLite/doltlite has FK enforcement OFF
# by default. Dolt has FK enforcement ON by default and does not accept
# the PRAGMA, so these are doltlite-only oracle cases (the matching Dolt
# behavior was verified manually during test authoring).
run_clean_merge_case() {
  local name="$1" setup_sql="$2" state_query="$3"
  local db="$TMPROOT/$name.db"
  rm -f "$db"

  printf '%s\n' "$setup_sql" | dl_setup "$db" "$name"

  local merge_err
  merge_err=$("$DOLTLITE" "$db" "SELECT dolt_merge('feat');" 2>"$TMPROOT/${name}_merge.err" > "$TMPROOT/${name}_merge.out"; cat "$TMPROOT/${name}_merge.err")
  if [ -n "$merge_err" ]; then
    fail_name "${name}_merge_succeeds"
    echo "    unexpected merge error: $merge_err"
    return
  else
    pass_name "${name}_merge_succeeds"
  fi

  local conflicts
  conflicts=$(dl "$db" "SELECT count(*) FROM dolt_conflicts;" "${name}_conflicts")
  expect_eq "${name}_no_conflicts" "0" "$conflicts"

  local violations
  violations=$(dl "$db" "SELECT count(*) FROM dolt_constraint_violations;" "${name}_violations")
  expect_eq "${name}_no_violations" "0" "$violations"

  local state
  state=$(dl "$db" "$state_query" "${name}_state")
  expect_eq "${name}_state_ok" "OK" "$state"
}

echo "--- I. ON DELETE CASCADE: parent dropped on main, unrelated feat insert ---"
run_clean_merge_case \
  "fk_action_cascade_clean" \
"PRAGMA foreign_keys=1;
CREATE TABLE parent(pk INTEGER PRIMARY KEY);
CREATE TABLE child(pk INTEGER PRIMARY KEY, pv INT, FOREIGN KEY(pv) REFERENCES parent(pk) ON DELETE CASCADE);
INSERT INTO parent VALUES (1),(2),(3);
INSERT INTO child VALUES (100,1),(101,1),(200,2);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO parent VALUES (4);
INSERT INTO child VALUES (400,4);
SELECT dolt_commit('-Am','feat_add_unrelated');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=1;
SELECT dolt_commit('-Am','main_drop_parent');" \
  "SELECT CASE
      WHEN (SELECT group_concat(pk || ':' || pv, ',')
              FROM (SELECT pk,pv FROM child ORDER BY pk))='200:2,400:4'
       AND (SELECT count(*) FROM parent)=3
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- J. ON DELETE CASCADE: orphan from feat side surfaces violation ---"
run_rollback_case \
  "fk_action_cascade_orphan" \
"PRAGMA foreign_keys=1;
CREATE TABLE parent(pk INTEGER PRIMARY KEY);
CREATE TABLE child(pk INTEGER PRIMARY KEY, pv INT, FOREIGN KEY(pv) REFERENCES parent(pk) ON DELETE CASCADE);
INSERT INTO parent VALUES (1),(2);
INSERT INTO child VALUES (100,1),(200,2);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (300,1);
SELECT dolt_commit('-Am','feat_add_child_of_p1');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=1;
SELECT dolt_commit('-Am','main_drop_p1');" \
  "SELECT CASE
      WHEN (SELECT count(*) FROM parent)=1
       AND (SELECT count(*) FROM child WHERE pv=1)=0
       AND (SELECT count(*) FROM child WHERE pk=200)=1
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- K. ON DELETE SET NULL: orphan from main side NULLed, feat add merges ---"
run_clean_merge_case \
  "fk_action_set_null_clean" \
"PRAGMA foreign_keys=1;
CREATE TABLE parent(pk INTEGER PRIMARY KEY);
CREATE TABLE child(pk INTEGER PRIMARY KEY, pv INT, FOREIGN KEY(pv) REFERENCES parent(pk) ON DELETE SET NULL);
INSERT INTO parent VALUES (1),(2);
INSERT INTO child VALUES (100,1),(200,2);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (300,2);
SELECT dolt_commit('-Am','feat_add_child');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=1;
SELECT dolt_commit('-Am','main_drop_p1');" \
  "SELECT CASE
      WHEN (SELECT group_concat(pk || ':' || ifnull(pv,'NULL'), ',')
              FROM (SELECT pk,pv FROM child ORDER BY pk))='100:NULL,200:2,300:2'
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- L. ON UPDATE CASCADE: parent key updated, propagates through merge ---"
run_clean_merge_case \
  "fk_action_update_cascade_clean" \
"PRAGMA foreign_keys=1;
CREATE TABLE parent(pk INTEGER PRIMARY KEY);
CREATE TABLE child(pk INTEGER PRIMARY KEY, pv INT, FOREIGN KEY(pv) REFERENCES parent(pk) ON UPDATE CASCADE);
INSERT INTO parent VALUES (1),(2);
INSERT INTO child VALUES (100,1),(200,2);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (300,2);
SELECT dolt_commit('-Am','feat_add_child');
SELECT dolt_checkout('main');
UPDATE parent SET pk=10 WHERE pk=1;
SELECT dolt_commit('-Am','main_renumber');" \
  "SELECT CASE
      WHEN (SELECT group_concat(pk || ':' || pv, ',')
              FROM (SELECT pk,pv FROM child ORDER BY pk))='100:10,200:2,300:2'
       AND (SELECT group_concat(pk, ',')
              FROM (SELECT pk FROM parent ORDER BY pk))='2,10'
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- M. Explicit ON DELETE NO ACTION: orphan from feat side rolls back ---"
run_rollback_case \
  "fk_action_no_action_explicit" \
"PRAGMA foreign_keys=1;
CREATE TABLE parent(pk INTEGER PRIMARY KEY);
CREATE TABLE child(pk INTEGER PRIMARY KEY, pv INT, FOREIGN KEY(pv) REFERENCES parent(pk) ON DELETE NO ACTION);
INSERT INTO parent VALUES (1),(2);
INSERT INTO child VALUES (200,2);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (300,1);
SELECT dolt_commit('-Am','feat_add_child_p1');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=1;
SELECT dolt_commit('-Am','main_drop_p1');" \
  "SELECT CASE
      WHEN (SELECT count(*) FROM parent)=1
       AND (SELECT count(*) FROM child)=1
       AND (SELECT count(*) FROM child WHERE pk=300)=0
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- N. Both sides delete same parent with CASCADE: convergent merge ---"
run_clean_merge_case \
  "fk_action_convergent_delete" \
"PRAGMA foreign_keys=1;
CREATE TABLE parent(pk INTEGER PRIMARY KEY);
CREATE TABLE child(pk INTEGER PRIMARY KEY, pv INT, FOREIGN KEY(pv) REFERENCES parent(pk) ON DELETE CASCADE);
INSERT INTO parent VALUES (1),(2);
INSERT INTO child VALUES (100,1),(200,2);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DELETE FROM parent WHERE pk=1;
SELECT dolt_commit('-Am','feat_drop_p1');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=1;
SELECT dolt_commit('-Am','main_drop_p1');" \
  "SELECT CASE
      WHEN (SELECT group_concat(pk, ',')
              FROM (SELECT pk FROM parent ORDER BY pk))='2'
       AND (SELECT group_concat(pk || ':' || pv, ',')
              FROM (SELECT pk,pv FROM child ORDER BY pk))='200:2'
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- O. Multi-column FK with ON DELETE CASCADE: clean merge ---"
run_clean_merge_case \
  "fk_action_multicolumn_cascade" \
"PRAGMA foreign_keys=1;
CREATE TABLE parent(a INT, b INT, PRIMARY KEY(a,b));
CREATE TABLE child(pk INTEGER PRIMARY KEY, ca INT, cb INT, FOREIGN KEY(ca,cb) REFERENCES parent(a,b) ON DELETE CASCADE);
INSERT INTO parent VALUES (1,1),(2,2);
INSERT INTO child VALUES (100,1,1),(200,2,2);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (300,2,2);
SELECT dolt_commit('-Am','feat_add_child');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE a=1 AND b=1;
SELECT dolt_commit('-Am','main_drop_p11');" \
  "SELECT CASE
      WHEN (SELECT group_concat(pk || ':' || ca || ':' || cb, ',')
              FROM (SELECT pk,ca,cb FROM child ORDER BY pk))='200:2:2,300:2:2'
       AND (SELECT count(*) FROM parent)=1
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- P. Multi-column FK with ON DELETE SET NULL: NULL both columns ---"
run_clean_merge_case \
  "fk_action_multicolumn_set_null" \
"PRAGMA foreign_keys=1;
CREATE TABLE parent(a INT, b INT, PRIMARY KEY(a,b));
CREATE TABLE child(pk INTEGER PRIMARY KEY, ca INT, cb INT, FOREIGN KEY(ca,cb) REFERENCES parent(a,b) ON DELETE SET NULL);
INSERT INTO parent VALUES (1,1),(2,2);
INSERT INTO child VALUES (100,1,1),(200,2,2);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (300,2,2);
SELECT dolt_commit('-Am','feat_add_child');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE a=1 AND b=1;
SELECT dolt_commit('-Am','main_drop_p11');" \
  "SELECT CASE
      WHEN (SELECT group_concat(pk || ':' || ifnull(ca,'NULL') || ':' || ifnull(cb,'NULL'), ',')
              FROM (SELECT pk,ca,cb FROM child ORDER BY pk))='100:NULL:NULL,200:2:2,300:2:2'
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- Q. Parent deleted on feat (not main) with CASCADE: clean merge ---"
run_clean_merge_case \
  "fk_action_cascade_feat_side" \
"PRAGMA foreign_keys=1;
CREATE TABLE parent(pk INTEGER PRIMARY KEY);
CREATE TABLE child(pk INTEGER PRIMARY KEY, pv INT, FOREIGN KEY(pv) REFERENCES parent(pk) ON DELETE CASCADE);
INSERT INTO parent VALUES (1),(2),(3);
INSERT INTO child VALUES (100,1),(200,2),(300,3);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DELETE FROM parent WHERE pk=2;
SELECT dolt_commit('-Am','feat_drop_p2');
SELECT dolt_checkout('main');
INSERT INTO parent VALUES (4);
INSERT INTO child VALUES (400,4);
SELECT dolt_commit('-Am','main_add_unrelated');" \
  "SELECT CASE
      WHEN (SELECT group_concat(pk || ':' || pv, ',')
              FROM (SELECT pk,pv FROM child ORDER BY pk))='100:1,300:3,400:4'
       AND (SELECT group_concat(pk, ',')
              FROM (SELECT pk FROM parent ORDER BY pk))='1,3,4'
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- R. ON UPDATE CASCADE + ON DELETE CASCADE: divergent edits surface conflict/violation and roll back ---"
run_rollback_case \
  "fk_action_update_divergent" \
"PRAGMA foreign_keys=1;
CREATE TABLE parent(pk INTEGER PRIMARY KEY);
CREATE TABLE child(pk INTEGER PRIMARY KEY, pv INT, FOREIGN KEY(pv) REFERENCES parent(pk) ON UPDATE CASCADE ON DELETE CASCADE);
INSERT INTO parent VALUES (1),(2);
INSERT INTO child VALUES (100,1),(200,2);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE child SET pv=2 WHERE pk=100;
SELECT dolt_commit('-Am','feat_repoint_child');
SELECT dolt_checkout('main');
UPDATE parent SET pk=10 WHERE pk=1;
SELECT dolt_commit('-Am','main_renumber_p1');" \
  "SELECT CASE
      WHEN (SELECT group_concat(pk, ',')
              FROM (SELECT pk FROM parent ORDER BY pk))='2,10'
       AND (SELECT group_concat(pk || ':' || pv, ',')
              FROM (SELECT pk,pv FROM child ORDER BY pk))='100:10,200:2'
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "--- S. ON DELETE SET DEFAULT (doltlite-only; dolt rejects the syntax, dolthub/dolt#11041) ---"
run_clean_merge_case \
  "fk_action_set_default" \
"PRAGMA foreign_keys=1;
CREATE TABLE parent(pk INTEGER PRIMARY KEY);
CREATE TABLE child(pk INTEGER PRIMARY KEY, pv INT DEFAULT 99, FOREIGN KEY(pv) REFERENCES parent(pk) ON DELETE SET DEFAULT);
INSERT INTO parent VALUES (1),(2),(99);
INSERT INTO child VALUES (100,1),(200,2);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (300,2);
SELECT dolt_commit('-Am','feat_add_child');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=1;
SELECT dolt_commit('-Am','main_drop_p1');" \
  "SELECT CASE
      WHEN (SELECT group_concat(pk || ':' || pv, ',')
              FROM (SELECT pk,pv FROM child ORDER BY pk))='100:99,200:2,300:2'
      THEN 'OK' ELSE 'BAD' END;"
echo ""

echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
