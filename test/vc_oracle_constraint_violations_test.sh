#!/bin/bash
#
# Oracle coverage for the dolt_constraint_violations_<table> and aggregate
# dolt_constraint_violations tables. Existing merge suites only assert
# count(*); this one compares the actual row contents against Dolt:
# violation_type, the offending row, and the semantic keys of violation_info
# (Columns / ReferencedTable / ReferencedColumns / Expression).
#
# FK and unique-index violations are oracled against Dolt. CHECK violations are
# not: producing one requires introducing a CHECK on one branch, which needs the
# SQLite recreate-and-rename trick, and Dolt then refuses the merge ("table has
# different primary keys"). So CHECK is asserted doltlite-only against a fixed
# expected value, matching vc_oracle_fk_merge_test.sh's split.
#
# violation_type is compared through a CASE map: Dolt stores it as an enum that
# CONCAT coerces to its ordinal, so a raw compare would diverge. The CASE label
# matches by enum label on Dolt and by text on doltlite. Multi-element
# violation_info arrays render as "[a, b]" on Dolt and "[a,b]" on doltlite, so
# normalize() collapses ", " to ",".

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

normalize() {
  tr -d '\r' | sed -E 's/, /,/g' | sort
}

oracle() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_script dl_out
  dl_script=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$query" \
              | perl -0pe "s/\nSELECT dolt_merge\\(/\nBEGIN;\\nSELECT dolt_merge\\(/")
  dl_out=$(printf "%s" "$dl_script" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^R|' \
           | tr -d '"' \
           | normalize)

  local dolt_all dt_out
  dolt_all=$(vc_oracle_translate_for_dolt "$(printf '%s\n%s' "$setup" "$query")")
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf 'SET @@autocommit = 0;\n'
      printf 'SET @@dolt_allow_commit_conflicts = 1;\n'
      printf '%s\n' "$dolt_all"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^R|' | normalize)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

dl_expect() {
  local name="$1" setup="$2" query="$3" expected="$4"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir"

  local dl_script dl_out
  dl_script=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$query" \
              | perl -0pe "s/\nSELECT dolt_merge\\(/\nBEGIN;\\nSELECT dolt_merge\\(/")
  dl_out=$(printf "%s" "$dl_script" \
           | "$DOLTLITE" "$dir/db" 2>"$dir/dl.err" \
           | grep '^R|' \
           | tr -d '"' \
           | normalize)

  if [ "$dl_out" = "$expected" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    expected:"; echo "$expected" | sed 's/^/      /'
  fi
}

echo "=== Version Control Oracle Tests: dolt_constraint_violations ==="
echo ""

echo "--- unique index: single column, two branches ---"
oracle "unique_single_col" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1,1,'base1'),(2,2,'base2');
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET u=9, v='feat2' WHERE id=2;
SELECT dolt_commit('-Am','feat_unique');
SELECT dolt_checkout('main');
UPDATE t SET u=9, v='main1' WHERE id=1;
SELECT dolt_commit('-Am','main_unique');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', CASE violation_type WHEN 'foreign key' THEN 'FK' WHEN 'unique index' THEN 'UQ' WHEN 'check constraint' THEN 'CK' ELSE '?' END, '|', id, '|', u, '|', v, '|cols=', JSON_EXTRACT(violation_info,'\$.Columns')) FROM dolt_constraint_violations_t ORDER BY id;
SELECT CONCAT('R|AGG|', \`table\`, '|', num_violations) FROM dolt_constraint_violations ORDER BY \`table\`;"

echo "--- unique index: composite (a,b) ---"
oracle "unique_composite" \
"CREATE TABLE t(pk INTEGER PRIMARY KEY, a INT, b INT, UNIQUE(a,b));
INSERT INTO t VALUES (1,1,1),(2,2,2);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET a=5, b=5 WHERE pk=2;
SELECT dolt_commit('-Am','feat_dup');
SELECT dolt_checkout('main');
UPDATE t SET a=5, b=5 WHERE pk=1;
SELECT dolt_commit('-Am','main_dup');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', CASE violation_type WHEN 'foreign key' THEN 'FK' WHEN 'unique index' THEN 'UQ' WHEN 'check constraint' THEN 'CK' ELSE '?' END, '|', pk, '|', a, '|', b, '|cols=', JSON_EXTRACT(violation_info,'\$.Columns')) FROM dolt_constraint_violations_t ORDER BY pk;
SELECT CONCAT('R|AGG|', \`table\`, '|', num_violations) FROM dolt_constraint_violations ORDER BY \`table\`;"

echo "--- foreign key: single orphan after parent delete ---"
oracle "fk_orphan_single" \
"CREATE TABLE p(pk INTEGER PRIMARY KEY, v INT UNIQUE);
CREATE TABLE c(pk INTEGER PRIMARY KEY, v INT, FOREIGN KEY(v) REFERENCES p(v));
INSERT INTO p VALUES (1,10),(2,20);
INSERT INTO c VALUES (1,10);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES (2,20);
SELECT dolt_commit('-Am','feat_child');
SELECT dolt_checkout('main');
DELETE FROM p WHERE v=20;
SELECT dolt_commit('-Am','main_delparent');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', CASE violation_type WHEN 'foreign key' THEN 'FK' WHEN 'unique index' THEN 'UQ' WHEN 'check constraint' THEN 'CK' ELSE '?' END, '|', pk, '|', v, '|cols=', JSON_EXTRACT(violation_info,'\$.Columns'), '|reft=', JSON_EXTRACT(violation_info,'\$.ReferencedTable'), '|refc=', JSON_EXTRACT(violation_info,'\$.ReferencedColumns')) FROM dolt_constraint_violations_c ORDER BY pk;
SELECT CONCAT('R|AGG|', \`table\`, '|', num_violations) FROM dolt_constraint_violations ORDER BY \`table\`;"

echo "--- foreign key: multiple orphans from one deleted parent ---"
oracle "fk_orphan_multi" \
"CREATE TABLE p(pk INTEGER PRIMARY KEY, v INT UNIQUE);
CREATE TABLE c(pk INTEGER PRIMARY KEY, v INT, FOREIGN KEY(v) REFERENCES p(v));
INSERT INTO p VALUES (1,10),(2,20);
INSERT INTO c VALUES (1,10);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES (2,20),(3,20);
SELECT dolt_commit('-Am','feat_children');
SELECT dolt_checkout('main');
DELETE FROM p WHERE v=20;
SELECT dolt_commit('-Am','main_delparent');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', CASE violation_type WHEN 'foreign key' THEN 'FK' WHEN 'unique index' THEN 'UQ' WHEN 'check constraint' THEN 'CK' ELSE '?' END, '|', pk, '|', v, '|cols=', JSON_EXTRACT(violation_info,'\$.Columns')) FROM dolt_constraint_violations_c ORDER BY pk;
SELECT CONCAT('R|AGG|', \`table\`, '|', num_violations) FROM dolt_constraint_violations ORDER BY \`table\`;"

echo "--- check constraint: violating row arrives on merge (doltlite-only) ---"
dl_expect "check_introduced_violating_row" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1,10);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2,-5);
SELECT dolt_commit('-Am','feat_row');
SELECT dolt_checkout('main');
CREATE TABLE t2(id INTEGER PRIMARY KEY, v INT CHECK(v>0));
INSERT INTO t2 SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t2 RENAME TO t;
SELECT dolt_commit('-Am','main_check');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', CASE violation_type WHEN 'foreign key' THEN 'FK' WHEN 'unique index' THEN 'UQ' WHEN 'check constraint' THEN 'CK' ELSE '?' END, '|', id, '|', v, '|expr=', JSON_EXTRACT(violation_info,'\$.Expression')) FROM dolt_constraint_violations_t ORDER BY id;
SELECT CONCAT('R|AGG|', 't|', num_violations) FROM dolt_constraint_violations;" \
"R|AGG|t|1
R|CK|2|-5|expr=v>0"

echo "--- not null: column tightened on one branch, NULL row on the other ---"
# Same split as CHECK above, and for the same reason: tightening a column to NOT
# NULL needs SQLite's recreate-and-rename, which Dolt reads as a primary-key
# change and refuses. Expressed Dolt's way (ALTER TABLE t MODIFY b ... NOT NULL)
# on Dolt 2.2.2, the merge reports one violation and
# dolt_constraint_violations_t holds exactly the row asserted here:
#   not null | 2 | {"Columns": ["b"]}
# so the expectation below is Dolt's answer, not just doltlite's.
dl_expect "not_null_tightened_other_branch_inserts_null" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, b TEXT);
INSERT INTO t VALUES (1,'x');
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2,NULL);
SELECT dolt_commit('-Am','feat_null');
SELECT dolt_checkout('main');
CREATE TABLE t2(id INTEGER PRIMARY KEY, b TEXT NOT NULL);
INSERT INTO t2 SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t2 RENAME TO t;
SELECT dolt_commit('-Am','main_not_null');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', CASE violation_type WHEN 'foreign key' THEN 'FK' WHEN 'unique index' THEN 'UQ' WHEN 'check constraint' THEN 'CK' WHEN 'not null' THEN 'NN' ELSE '?' END, '|', id, '|cols=', JSON_EXTRACT(violation_info,'\$.Columns')) FROM dolt_constraint_violations_t ORDER BY id;
SELECT CONCAT('R|AGG|', 't|', num_violations) FROM dolt_constraint_violations;" \
"R|AGG|t|1
R|NN|2|cols=[b]"

# A column added NOT NULL WITH a default is not a violation on either engine:
# the rows merged in from the other branch take the default. Asserted so the
# detector cannot start reporting those.
dl_expect "not_null_added_with_default_is_clean" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a');
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2,'b');
SELECT dolt_commit('-Am','feat_row');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN c TEXT NOT NULL DEFAULT 7;
SELECT dolt_commit('-Am','main_addcol');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|CV|', count(*)) FROM dolt_constraint_violations_t;
SELECT CONCAT('R|ROW|', id, '|', quote(c)) FROM t ORDER BY id;" \
"R|CV|0
R|ROW|1|'7'
R|ROW|2|'7'"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
