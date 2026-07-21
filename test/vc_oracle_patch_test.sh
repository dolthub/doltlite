#!/usr/bin/env bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0
fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

# Compare the operation stream and normalized data SQL with Dolt. Identifier
# quotes and insignificant spaces are removed before hex encoding so CSV
# parsing cannot confuse commas inside generated statements.
oracle_data() {
  local name="$1" setup="$2" args="$3" where="${4:-1}"
  local dir="$TMPROOT/$name"
  local dl_out dt_out dolt_setup
  mkdir -p "$dir/dl" "$dir/dt"

  dl_out=$(
    {
      printf '%s\n' "$setup"
      printf "%s\n" ".mode list" ".separator |" \
        "SELECT 'P',statement_order,table_name,diff_type,lower(hex(replace(replace(statement,'\"',''),' ',''))) FROM dolt_patch($args) WHERE $where;"
    } | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" | grep '^P|' || true
  )

  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "$dolt_setup"
      printf "%s\n" \
        "SELECT concat('P|',statement_order,'|',table_name,'|',diff_type,'|',lower(hex(replace(replace(statement,char(96),''),' ','')))) FROM dolt_patch($args) WHERE $where;"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" | tr -d '"' | grep '^P|' || true
  )
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_shape() {
  local name="$1" setup="$2" args="$3" projection="$4"
  local dir="$TMPROOT/$name"
  local dl_out dt_out dolt_setup
  mkdir -p "$dir/dl" "$dir/dt"
  dl_out=$(
    {
      printf '%s\n' "$setup"
      printf "%s\n" ".mode list" ".separator |" \
        "SELECT 'P',$projection FROM dolt_patch($args);"
    } | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" | grep '^P|' || true
  )
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "$dolt_setup"
      printf "%s\n" "SELECT concat('P|',concat_ws('|',$projection)) FROM dolt_patch($args);"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" | tr -d '"' | grep '^P|' || true
  )
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_error() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/$name"
  local dl_rc dt_rc dolt_setup
  mkdir -p "$dir/dl" "$dir/dt"
  printf '%s\n%s\n' "$setup" "$query" | "$DOLTLITE" "$dir/dl/db" \
    >"$dir/dl.out" 2>"$dir/dl.err"
  dl_rc=$?
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf '%s\n%s\n' "$dolt_setup" "$query" | "$DOLT" sql \
      >"$dir/dt.out" 2>"$dir/dt.err"
  )
  dt_rc=$?
  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (doltlite rc=$dl_rc, dolt rc=$dt_rc)"
  fi
}

basic_setup="
CREATE TABLE t(pk INTEGER PRIMARY KEY, c1 TEXT, n INTEGER);
INSERT INTO t VALUES (1,'one',10),(2,'two',20),(4,NULL,40);
SELECT dolt_commit('-A','-m','base');
UPDATE t SET c1='uno',n=11 WHERE pk=1;
DELETE FROM t WHERE pk=2;
INSERT INTO t VALUES (3,'three',30);
SELECT dolt_commit('-A','-m','next');
"
oracle_data basic_insert_update_delete "$basic_setup" "'HEAD~1','HEAD'" "diff_type='data'"
oracle_data reverse_insert_update_delete "$basic_setup" "'HEAD','HEAD~1'" "diff_type='data'"
oracle_data two_dot_range "$basic_setup" "'HEAD~1..HEAD'" "diff_type='data'"
oracle_data range_with_table "$basic_setup" "'HEAD~1..HEAD','t'" "diff_type='data'"
oracle_data explicit_table "$basic_setup" "'HEAD~1','HEAD','t'" "diff_type='data'"

oracle_data multi_table_order "
CREATE TABLE z(pk INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE a(pk INTEGER PRIMARY KEY, v TEXT);
INSERT INTO z VALUES (1,'z');
INSERT INTO a VALUES (1,'a');
SELECT dolt_commit('-A','-m','base');
UPDATE z SET v='zee' WHERE pk=1;
UPDATE a SET v='aye' WHERE pk=1;
SELECT dolt_commit('-A','-m','next');
" "'HEAD~1','HEAD'" "diff_type='data'"

oracle_data added_table_rows "
CREATE TABLE seed(pk INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','base');
CREATE TABLE added(pk INTEGER PRIMARY KEY, v TEXT);
INSERT INTO added VALUES (1,'a'),(2,'b');
SELECT dolt_commit('-A','-m','next');
" "'HEAD~1','HEAD'" "diff_type='data'"

oracle_shape dropped_table_has_no_data "
CREATE TABLE gone(pk INTEGER PRIMARY KEY, v TEXT);
INSERT INTO gone VALUES (1,'a');
SELECT dolt_commit('-A','-m','base');
DROP TABLE gone;
SELECT dolt_commit('-A','-m','next');
" "'HEAD~1','HEAD'" "table_name,diff_type"

oracle_data composite_primary_key "
CREATE TABLE t(a VARCHAR(20), b INTEGER, v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES ('x',1,'old'),('x',2,'delete');
SELECT dolt_commit('-A','-m','base');
UPDATE t SET v='new' WHERE a='x' AND b=1;
DELETE FROM t WHERE a='x' AND b=2;
INSERT INTO t VALUES ('y',3,'add');
SELECT dolt_commit('-A','-m','next');
" "'HEAD~1','HEAD'" "diff_type='data'"

oracle_shape working_label "
CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a');
SELECT dolt_commit('-A','-m','base');
UPDATE t SET v='working' WHERE pk=1;
" "'HEAD','WORKING'" "table_name,diff_type,to_commit_hash"

oracle_shape staged_label "
CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a');
SELECT dolt_commit('-A','-m','base');
UPDATE t SET v='staged' WHERE pk=1;
SELECT dolt_add('t');
" "'HEAD','STAGED'" "table_name,diff_type,to_commit_hash"

oracle_data three_dot_merge_base "
CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'base');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2,'feature');
SELECT dolt_commit('-A','-m','feature');
SELECT dolt_checkout('main');
UPDATE t SET v='main' WHERE pk=1;
SELECT dolt_commit('-A','-m','main');
" "'feature...main'" "diff_type='data'"

error_setup="
CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','base');
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','next');
"
oracle_error no_arguments "$error_setup" "SELECT * FROM dolt_patch();"
oracle_error one_non_range_argument "$error_setup" "SELECT * FROM dolt_patch('t');"
oracle_error bad_from_ref "$error_setup" "SELECT * FROM dolt_patch('missing-ref','HEAD','t');"
oracle_error bad_to_ref "$error_setup" "SELECT * FROM dolt_patch('HEAD','missing-ref','t');"
oracle_error missing_table "$error_setup" "SELECT * FROM dolt_patch('HEAD~1','HEAD','missing_table');"

# SQLite-specific verification: generate a schema+data patch, reset to the
# source revision, execute every statement, and compare the target fingerprint.
apply_dir="$TMPROOT/apply_schema"
mkdir -p "$apply_dir"
apply_db="$apply_dir/db"
{
  printf '%s\n' \
    "CREATE TABLE t(pk INTEGER PRIMARY KEY,c1 TEXT);" \
    "INSERT INTO t VALUES(1,'one'),(2,'two');" \
    "CREATE INDEX ix_t_c1 ON t(c1);" \
    "SELECT dolt_commit('-A','-m','base');" \
    "ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 7;" \
    "UPDATE t SET c1='uno',n=8 WHERE pk=1;" \
    "DROP INDEX ix_t_c1;" \
    "CREATE UNIQUE INDEX ix_t_c1 ON t(c1);" \
    "CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET n=9 WHERE pk=new.pk; END;" \
    "SELECT dolt_commit('-A','-m','target');"
} | "$DOLTLITE" "$apply_db" >/dev/null
target_fingerprint=$("$DOLTLITE" "$apply_db" \
  "SELECT group_concat(pk||':'||c1||':'||n,',') FROM (SELECT * FROM t ORDER BY pk);" \
  "SELECT group_concat(type||':'||name,',') FROM (SELECT type,name FROM sqlite_master WHERE tbl_name='t' ORDER BY type,name);")
"$DOLTLITE" "$apply_db" "SELECT statement FROM dolt_patch('HEAD~1','HEAD');" >"$apply_dir/patch.sql"
"$DOLTLITE" "$apply_db" "SELECT dolt_reset('--hard','HEAD~1');" >/dev/null
if "$DOLTLITE" "$apply_db" <"$apply_dir/patch.sql" >"$apply_dir/apply.out" 2>"$apply_dir/apply.err"; then
  actual_fingerprint=$("$DOLTLITE" "$apply_db" \
    "SELECT group_concat(pk||':'||c1||':'||n,',') FROM (SELECT * FROM t ORDER BY pk);" \
    "SELECT group_concat(type||':'||name,',') FROM (SELECT type,name FROM sqlite_master WHERE tbl_name='t' ORDER BY type,name);")
else
  actual_fingerprint="apply failed: $(cat "$apply_dir/apply.err")"
fi
vc_oracle_assert_match apply_schema_and_data "$actual_fingerprint" "$target_fingerprint"

# Literal round-trip covers quotes, embedded NUL text, blobs, reals and NULL.
literal_dir="$TMPROOT/apply_literals"
mkdir -p "$literal_dir"
literal_db="$literal_dir/db"
{
  printf '%s\n' \
    "CREATE TABLE t(pk INTEGER PRIMARY KEY, txt TEXT, b BLOB, r REAL, n TEXT);" \
    "SELECT dolt_commit('-A','-m','base');" \
    "INSERT INTO t VALUES(1,'it''s'||char(0)||'nul',x'00ff10',1.25,NULL);" \
    "SELECT dolt_commit('-A','-m','target');"
} | "$DOLTLITE" "$literal_db" >/dev/null
target_fingerprint=$("$DOLTLITE" "$literal_db" \
  "SELECT pk||'|'||hex(txt)||'|'||hex(b)||'|'||printf('%.17g',r)||'|'||(n IS NULL) FROM t;")
"$DOLTLITE" "$literal_db" "SELECT statement FROM dolt_patch('HEAD~1','HEAD');" >"$literal_dir/patch.sql"
"$DOLTLITE" "$literal_db" "SELECT dolt_reset('--hard','HEAD~1');" >/dev/null
if "$DOLTLITE" "$literal_db" <"$literal_dir/patch.sql" 2>"$literal_dir/apply.err"; then
  actual_fingerprint=$("$DOLTLITE" "$literal_db" \
    "SELECT pk||'|'||hex(txt)||'|'||hex(b)||'|'||printf('%.17g',r)||'|'||(n IS NULL) FROM t;")
else
  actual_fingerprint="apply failed: $(cat "$literal_dir/apply.err")"
fi
vc_oracle_assert_match apply_literal_round_trip "$actual_fingerprint" "$target_fingerprint"

# A pure table rename should be represented as a rebuild that remains
# executable, including quoted identifiers and an index recreated under the
# new table name.
rename_dir="$TMPROOT/apply_rename"
mkdir -p "$rename_dir"
rename_db="$rename_dir/db"
{
  printf '%s\n' \
    'CREATE TABLE "odd table"("pk col" TEXT PRIMARY KEY, "value" TEXT);' \
    'INSERT INTO "odd table" VALUES('"'"'a'"'"','"'"'one'"'"'),('"'"'b'"'"','"'"'two'"'"');' \
    'CREATE INDEX "odd index" ON "odd table"("value");' \
    "SELECT dolt_commit('-A','-m','base');" \
    'ALTER TABLE "odd table" RENAME TO "new table";' \
    "SELECT dolt_commit('-A','-m','target');"
} | "$DOLTLITE" "$rename_db" >/dev/null
target_fingerprint=$("$DOLTLITE" "$rename_db" \
  "SELECT group_concat(\"pk col\"||':'||\"value\",',') FROM (SELECT * FROM \"new table\" ORDER BY \"pk col\");" \
  "SELECT group_concat(type||':'||name,',') FROM (SELECT type,name FROM sqlite_master WHERE tbl_name='new table' ORDER BY type,name);")
"$DOLTLITE" "$rename_db" "SELECT statement FROM dolt_patch('HEAD~1','HEAD');" >"$rename_dir/patch.sql"
"$DOLTLITE" "$rename_db" "SELECT dolt_reset('--hard','HEAD~1');" >/dev/null
if "$DOLTLITE" "$rename_db" <"$rename_dir/patch.sql" 2>"$rename_dir/apply.err"; then
  actual_fingerprint=$("$DOLTLITE" "$rename_db" \
    "SELECT group_concat(\"pk col\"||':'||\"value\",',') FROM (SELECT * FROM \"new table\" ORDER BY \"pk col\");" \
    "SELECT group_concat(type||':'||name,',') FROM (SELECT type,name FROM sqlite_master WHERE tbl_name='new table' ORDER BY type,name);")
else
  actual_fingerprint="apply failed: $(cat "$rename_dir/apply.err")"
fi
vc_oracle_assert_match apply_table_rename "$actual_fingerprint" "$target_fingerprint"

# Ported from Dolt's WORKING/STAGED schema test: rename, drop, and add columns
# together, then prove both forward and reverse generated patches execute.
columns_dir="$TMPROOT/apply_columns"
mkdir -p "$columns_dir"
columns_db="$columns_dir/db"
{
  printf '%s\n' \
    "CREATE TABLE t(pk INTEGER PRIMARY KEY,c1 INTEGER,c2 INTEGER,c3 INTEGER,c4 INTEGER,c5 INTEGER);" \
    "INSERT INTO t VALUES(0,1,2,3,4,5),(1,1,2,3,4,5);" \
    "SELECT dolt_commit('-A','-m','base');" \
    "ALTER TABLE t RENAME COLUMN c1 TO c0;" \
    "ALTER TABLE t DROP COLUMN c4;" \
    "ALTER TABLE t ADD COLUMN c6 INTEGER;" \
    "UPDATE t SET c6=60 WHERE pk=1;" \
    "SELECT dolt_commit('-A','-m','target');"
} | "$DOLTLITE" "$columns_db" >/dev/null
target_fingerprint=$("$DOLTLITE" "$columns_db" \
  "SELECT group_concat(pk||':'||c0||':'||c2||':'||c3||':'||c5||':'||coalesce(c6,'NULL'),',') FROM (SELECT * FROM t ORDER BY pk);")
"$DOLTLITE" "$columns_db" "SELECT statement FROM dolt_patch('HEAD~1','HEAD');" >"$columns_dir/forward.sql"
"$DOLTLITE" "$columns_db" "SELECT dolt_reset('--hard','HEAD~1');" >/dev/null
if "$DOLTLITE" "$columns_db" <"$columns_dir/forward.sql" 2>"$columns_dir/apply.err"; then
  actual_fingerprint=$("$DOLTLITE" "$columns_db" \
    "SELECT group_concat(pk||':'||c0||':'||c2||':'||c3||':'||c5||':'||coalesce(c6,'NULL'),',') FROM (SELECT * FROM t ORDER BY pk);")
else
  actual_fingerprint="apply failed: $(cat "$columns_dir/apply.err")"
fi
vc_oracle_assert_match apply_column_rename_drop_add "$actual_fingerprint" "$target_fingerprint"

echo ""
echo "=== dolt_patch oracle results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
