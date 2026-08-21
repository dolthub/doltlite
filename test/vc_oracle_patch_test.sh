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

# Strip identifier quotes/spaces before hex so CSV cannot split on commas in SQL.
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
  local name="$1" setup="$2" args="$3" projection="$4" where="${5:-1}"
  local dir="$TMPROOT/$name"
  local dl_out dt_out dolt_setup
  mkdir -p "$dir/dl" "$dir/dt"
  dl_out=$(
    {
      printf '%s\n' "$setup"
      printf "%s\n" ".mode list" ".separator |" \
        "SELECT 'P',$projection FROM dolt_patch($args) WHERE $where;"
    } | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" | grep '^P|' || true
  )
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "$dolt_setup"
      printf "%s\n" "SELECT concat('P|',concat_ws('|',$projection)) FROM dolt_patch($args) WHERE $where;"
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

# Both patch directions vs a fingerprint. setup must create tags base and target.
apply_bidirectional() {
  local name="$1" setup="$2" fingerprint_sql="$3"
  local dir="$TMPROOT/apply_$name"
  local db="$TMPROOT/apply_$name/forward.db"
  local reverse_db="$TMPROOT/apply_$name/reverse.db"
  local base_fingerprint target_fingerprint actual
  mkdir -p "$dir"
  if ! printf '%s\n' "$setup" | "$DOLTLITE" "$db" \
      >"$dir/setup.out" 2>"$dir/setup.err"; then
    vc_oracle_assert_match "${name}_setup" \
      "setup failed: $(cat "$dir/setup.err")" "setup succeeded"
    return
  fi
  if ! printf '%s\n' "$setup" | "$DOLTLITE" "$reverse_db" \
      >"$dir/reverse_setup.out" 2>"$dir/reverse_setup.err"; then
    vc_oracle_assert_match "${name}_reverse_setup" \
      "setup failed: $(cat "$dir/reverse_setup.err")" "setup succeeded"
    return
  fi
  if ! "$DOLTLITE" "$db" \
      "SELECT statement FROM dolt_patch('base','target');" \
      >"$dir/forward.sql" 2>"$dir/forward.err"; then
    vc_oracle_assert_match "${name}_forward_generation" \
      "generation failed: $(cat "$dir/forward.err")" "generation succeeded"
    return
  fi
  if ! "$DOLTLITE" "$db" \
      "SELECT statement FROM dolt_patch('target','base');" \
      >"$dir/reverse.sql" 2>"$dir/reverse.err"; then
    vc_oracle_assert_match "${name}_reverse_generation" \
      "generation failed: $(cat "$dir/reverse.err")" "generation succeeded"
    return
  fi

  target_fingerprint=$("$DOLTLITE" "$db" "$fingerprint_sql" 2>"$dir/target.err")
  "$DOLTLITE" "$db" "SELECT dolt_reset('--hard','base');" >/dev/null
  base_fingerprint=$("$DOLTLITE" "$db" "$fingerprint_sql" 2>"$dir/base.err")

  if "$DOLTLITE" "$db" <"$dir/forward.sql" >"$dir/forward.out" 2>"$dir/forward_apply.err"; then
    actual=$("$DOLTLITE" "$db" "$fingerprint_sql" 2>"$dir/forward_actual.err")
  else
    actual="apply failed: $(cat "$dir/forward_apply.err")"
  fi
  vc_oracle_assert_match "${name}_forward" "$actual" "$target_fingerprint"

  if "$DOLTLITE" "$reverse_db" <"$dir/reverse.sql" \
      >"$dir/reverse.out" 2>"$dir/reverse_apply.err"; then
    actual=$("$DOLTLITE" "$reverse_db" "$fingerprint_sql" \
      2>"$dir/reverse_actual.err")
  else
    actual="apply failed: $(cat "$dir/reverse_apply.err")"
  fi
  vc_oracle_assert_match "${name}_reverse" "$actual" "$base_fingerprint"
}

apply_forward() {
  local name="$1" setup="$2" fingerprint_sql="$3"
  local dir="$TMPROOT/apply_$name"
  local db="$dir/db"
  local target_fingerprint actual
  mkdir -p "$dir"
  if ! printf '%s\n' "$setup" | "$DOLTLITE" "$db" \
      >"$dir/setup.out" 2>"$dir/setup.err"; then
    vc_oracle_assert_match "${name}_setup" \
      "setup failed: $(cat "$dir/setup.err")" "setup succeeded"
    return
  fi
  if ! "$DOLTLITE" "$db" \
      "SELECT statement FROM dolt_patch('base','target');" \
      >"$dir/forward.sql" 2>"$dir/forward.err"; then
    vc_oracle_assert_match "${name}_generation" \
      "generation failed: $(cat "$dir/forward.err")" "generation succeeded"
    return
  fi
  target_fingerprint=$("$DOLTLITE" "$db" "$fingerprint_sql" \
    2>"$dir/target.err")
  "$DOLTLITE" "$db" "SELECT dolt_reset('--hard','base');" >/dev/null
  if "$DOLTLITE" "$db" <"$dir/forward.sql" \
      >"$dir/forward.out" 2>"$dir/forward_apply.err"; then
    actual=$("$DOLTLITE" "$db" "$fingerprint_sql" \
      2>"$dir/forward_actual.err")
  else
    actual="apply failed: $(cat "$dir/forward_apply.err")"
  fi
  vc_oracle_assert_match "$name" "$actual" "$target_fingerprint"
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
oracle_data case_insensitive_table "$basic_setup" "'HEAD~1','HEAD','T'" \
  "diff_type='data'"
oracle_shape data_filter_count "$basic_setup" "'HEAD~1','HEAD'" "count(*)" \
  "diff_type='data'"
oracle_shape schema_filter_count "$basic_setup" "'HEAD~1','HEAD'" "count(*)" \
  "diff_type='schema'"
oracle_shape unknown_filter_empty "$basic_setup" "'HEAD~1','HEAD'" "count(*)" \
  "diff_type='unknown'"

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

# Explicit, two-dot, and both three-dot directions vs Dolt.
branch_matrix_setup="
CREATE TABLE t(pk INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT);
INSERT INTO t VALUES(1,'one','two');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('branch1');
INSERT INTO t VALUES(2,'main','row');
SELECT dolt_commit('-A','-m','main change');
CREATE TABLE newtable(pk INTEGER PRIMARY KEY);
INSERT INTO newtable VALUES(1),(2);
SELECT dolt_commit('-A','-m','main table');
SELECT dolt_checkout('branch1');
ALTER TABLE t DROP COLUMN c2;
DELETE FROM t WHERE pk=1;
INSERT INTO t VALUES(3,'branch');
SELECT dolt_commit('-A','-m','branch change');
"
oracle_data branch_explicit_main_to_branch "$branch_matrix_setup" \
  "'main','branch1'" "diff_type='data'"
oracle_data branch_two_dot_main_to_branch "$branch_matrix_setup" \
  "'main..branch1'" "diff_type='data'"
oracle_data branch_explicit_reverse "$branch_matrix_setup" \
  "'branch1','main'" "diff_type='data'"
oracle_data branch_two_dot_reverse "$branch_matrix_setup" \
  "'branch1..main'" "diff_type='data'"
oracle_data branch_three_dot_to_branch "$branch_matrix_setup" \
  "'main...branch1'" "diff_type='data'"
oracle_data branch_three_dot_to_main "$branch_matrix_setup" \
  "'branch1...main'" "diff_type='data'"

# Dolt accepts both old and new names as a rename filter (including new data).
rename_oracle_setup="
CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER);
INSERT INTO t1 VALUES(1,2);
SELECT dolt_commit('-A','-m','base');
ALTER TABLE t1 RENAME TO t2;
INSERT INTO t2 VALUES(3,4);
SELECT dolt_commit('-A','-m','renamed');
"
oracle_data rename_filter_new_name "$rename_oracle_setup" \
  "'HEAD~1','HEAD','t2'" "diff_type='data'"
oracle_data rename_filter_old_name "$rename_oracle_setup" \
  "'HEAD~1..HEAD','t1'" "diff_type='data'"

# HEAD/STAGED/WORKING are independently addressable in either direction.
workspace_matrix_setup="
CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','base');
UPDATE t SET v='staged' WHERE pk=1;
SELECT dolt_add('t');
UPDATE t SET v='working' WHERE pk=1;
"
oracle_data head_to_staged "$workspace_matrix_setup" \
  "'HEAD','STAGED'" "diff_type='data'"
oracle_data staged_to_working "$workspace_matrix_setup" \
  "'STAGED','WORKING'" "diff_type='data'"
oracle_data working_to_staged "$workspace_matrix_setup" \
  "'WORKING','STAGED'" "diff_type='data'"
oracle_shape working_to_working_empty "$workspace_matrix_setup" \
  "'WORKING','WORKING'" "count(*)"
oracle_shape staged_to_staged_empty "$workspace_matrix_setup" \
  "'STAGED..STAGED'" "count(*)"

# Enough ordered changes to span prolly chunks; compare exact statement order.
scale_setup="CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT);"
scale_values=""
for i in $(seq 1 400); do
  [ -n "$scale_values" ] && scale_values="$scale_values,"
  scale_values="${scale_values}($i,'value$i')"
done
scale_setup="$scale_setup INSERT INTO t VALUES $scale_values;
SELECT dolt_commit('-A','-m','base');
UPDATE t SET v=concat('changed',pk) WHERE pk%10=0;
DELETE FROM t WHERE pk%10=1;"
scale_values=""
for i in $(seq 401 450); do
  [ -n "$scale_values" ] && scale_values="$scale_values,"
  scale_values="${scale_values}($i,'value$i')"
done
scale_setup="$scale_setup INSERT INTO t VALUES $scale_values;
SELECT dolt_commit('-A','-m','target');"
oracle_data multi_chunk_ordering "$scale_setup" "'HEAD~1','HEAD'" "diff_type='data'"

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
oracle_error empty_range_right "$error_setup" "SELECT * FROM dolt_patch('HEAD~1..');"
oracle_error empty_range_left "$error_setup" "SELECT * FROM dolt_patch('..HEAD');"
oracle_error four_arguments "$error_setup" \
  "SELECT * FROM dolt_patch('HEAD~1','HEAD','t','extra');"
oracle_error null_arguments "$error_setup" \
  "SELECT * FROM dolt_patch(NULL,NULL,NULL);"
oracle_error numeric_from_ref "$error_setup" \
  "SELECT * FROM dolt_patch(123,'HEAD','t');"
oracle_error numeric_to_ref "$error_setup" \
  "SELECT * FROM dolt_patch('HEAD~1',123,'t');"
oracle_error numeric_table "$error_setup" \
  "SELECT * FROM dolt_patch('HEAD~1','HEAD',123);"
oracle_error bad_range_from_ref "$error_setup" \
  "SELECT * FROM dolt_patch('missing-ref..HEAD','t');"
oracle_error bad_range_to_ref "$error_setup" \
  "SELECT * FROM dolt_patch('HEAD~1..missing-ref','t');"

# Apply generated patch after reset; compare the target fingerprint.
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

# Quotes, embedded NUL, blobs, reals, NULL.
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

# Table rename as an executable rebuild, including quoted ids and the index.
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

# Rename/drop/add columns; both generated directions must execute.
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

# Record layouts and DDL that statement comparison cannot validate.
apply_bidirectional table_add_drop_multi_table "
CREATE TABLE common(pk INTEGER PRIMARY KEY,v TEXT);
CREATE TABLE old_table(pk INTEGER PRIMARY KEY,v TEXT);
INSERT INTO common VALUES(1,'base');
INSERT INTO old_table VALUES(1,'old-a'),(2,'old-b');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
DROP TABLE old_table;
CREATE TABLE new_table(pk INTEGER PRIMARY KEY,v TEXT);
INSERT INTO new_table VALUES(10,'new-a'),(20,'new-b');
UPDATE common SET v='target' WHERE pk=1;
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(name||':'||sql,';') FROM (
  SELECT name,sql FROM sqlite_master
  WHERE type='table' AND name NOT LIKE 'dolt_%' AND name NOT LIKE 'sqlite_%'
  ORDER BY name
);
SELECT count(*) FROM dolt_diff('base','WORKING');
SELECT count(*) FROM dolt_diff('target','WORKING');
"

apply_bidirectional without_rowid_pk_only "
CREATE TABLE t(a TEXT,b INTEGER,PRIMARY KEY(a,b)) WITHOUT ROWID;
INSERT INTO t VALUES('a',1),('b',2),('c',3);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
DELETE FROM t WHERE a='b' AND b=2;
INSERT INTO t VALUES('d',4);
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(a||':'||b,',') FROM (SELECT * FROM t ORDER BY a,b);
SELECT sql FROM sqlite_master WHERE name='t';
"

apply_bidirectional keyless_rowid_reuse "
CREATE TABLE t(a TEXT,b INTEGER);
INSERT INTO t VALUES('same',1),('same',1),('keep',2);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
UPDATE t SET b=9 WHERE rowid=1;
DELETE FROM t WHERE rowid=2;
INSERT INTO t VALUES('new',3);
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(rowid||':'||a||':'||b,',') FROM (SELECT rowid,* FROM t ORDER BY rowid);
SELECT sql FROM sqlite_master WHERE name='t';
"

apply_bidirectional keyless_rowid_alias_shadow "
CREATE TABLE t(rowid TEXT,v INTEGER);
INSERT INTO t(rowid,v) VALUES('declared-a',1),('declared-b',2);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
UPDATE t SET v=11 WHERE _rowid_=1;
DELETE FROM t WHERE _rowid_=2;
INSERT INTO t(rowid,v) VALUES('declared-c',3);
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(_rowid_||':'||rowid||':'||v,',') FROM (SELECT _rowid_,* FROM t ORDER BY _rowid_);
SELECT sql FROM sqlite_master WHERE name='t';
"

apply_bidirectional blob_composite_primary_key "
CREATE TABLE t(k BLOB,s TEXT,v BLOB,n REAL,PRIMARY KEY(k,s)) WITHOUT ROWID;
INSERT INTO t VALUES(x'00ff','a',x'01',1.25),(x'10','b',x'02',-4.5);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
UPDATE t SET v=x'abcdef',n=3.141592653589793 WHERE k=x'00ff' AND s='a';
DELETE FROM t WHERE k=x'10' AND s='b';
INSERT INTO t VALUES(x'80','quote''s',x'00ff10',0.0);
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(hex(k)||':'||hex(s)||':'||hex(v)||':'||printf('%.17g',n),',')
  FROM (SELECT * FROM t ORDER BY k,s);
SELECT sql FROM sqlite_master WHERE name='t';
"

apply_bidirectional generated_columns "
CREATE TABLE t(
  pk INTEGER PRIMARY KEY,
  a INTEGER,
  doubled INTEGER GENERATED ALWAYS AS (a*2) VIRTUAL,
  shifted INTEGER GENERATED ALWAYS AS (a+3) STORED
);
INSERT INTO t(pk,a) VALUES(1,5),(2,7);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
UPDATE t SET a=11 WHERE pk=1;
DELETE FROM t WHERE pk=2;
INSERT INTO t(pk,a) VALUES(3,13);
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(pk||':'||a||':'||doubled||':'||shifted,',') FROM (SELECT * FROM t ORDER BY pk);
SELECT sql FROM sqlite_master WHERE name='t';
"

apply_bidirectional strict_schema_replacement "
CREATE TABLE t(pk INTEGER PRIMARY KEY,v TEXT);
INSERT INTO t VALUES(1,'one'),(2,'two');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
CREATE TABLE t_new(
  pk INTEGER PRIMARY KEY,
  v TEXT,
  n INTEGER NOT NULL DEFAULT 7 CHECK(n>0)
) STRICT;
INSERT INTO t_new(pk,v) SELECT pk,v FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
UPDATE t SET v='uno',n=8 WHERE pk=1;
INSERT INTO t(pk,v,n) VALUES(3,'three',9);
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT dolt_hashof_table('t');
SELECT sql FROM sqlite_master WHERE name='t';
"

apply_bidirectional primary_key_replacement "
CREATE TABLE t(a TEXT PRIMARY KEY,b INTEGER,v TEXT);
INSERT INTO t VALUES('a',1,'one'),('b',2,'two');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
CREATE TABLE t_new(a TEXT,b INTEGER,v TEXT,PRIMARY KEY(a,b)) WITHOUT ROWID;
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
UPDATE t SET v='uno' WHERE a='a' AND b=1;
DELETE FROM t WHERE a='b' AND b=2;
INSERT INTO t VALUES('c',3,'three');
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT dolt_hashof_table('t');
SELECT sql FROM sqlite_master WHERE name='t';
"

apply_bidirectional integer_to_text_primary_key "
CREATE TABLE t(pk INTEGER PRIMARY KEY,v TEXT);
INSERT INTO t VALUES(1,'one'),(2,'two');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
CREATE TABLE t_new(pk TEXT PRIMARY KEY,v TEXT);
INSERT INTO t_new SELECT CAST(pk AS TEXT),v FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
UPDATE t SET v='uno' WHERE pk='1';
DELETE FROM t WHERE pk='2';
INSERT INTO t VALUES('03','three');
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT dolt_hashof_table('t');
SELECT sql FROM sqlite_master WHERE name='t';
"

apply_bidirectional index_matrix "
CREATE TABLE t(pk INTEGER PRIMARY KEY,a INTEGER,b TEXT);
INSERT INTO t VALUES(1,10,'one'),(2,20,'two'),(3,30,'three');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
CREATE UNIQUE INDEX ux_t_a ON t(a);
CREATE INDEX ix_t_expr ON t(lower(b));
CREATE INDEX ix_t_partial ON t(b) WHERE a>=20;
UPDATE t SET b='TWO' WHERE pk=2;
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(pk||':'||a||':'||b,',') FROM (SELECT * FROM t ORDER BY pk);
SELECT group_concat(name||':'||sql,';') FROM (SELECT name,sql FROM sqlite_master WHERE type='index' AND tbl_name='t' ORDER BY name);
"

apply_bidirectional trigger_side_effects "
CREATE TABLE audit(msg TEXT);
CREATE TABLE t(pk INTEGER PRIMARY KEY,v INTEGER);
INSERT INTO t VALUES(1,5);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
CREATE TRIGGER tr_t_insert AFTER INSERT ON t BEGIN
  UPDATE t SET v=v+1 WHERE pk=new.pk;
  INSERT INTO audit VALUES('insert:'||new.pk);
END;
INSERT INTO t VALUES(2,10);
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(pk||':'||v,',') FROM (SELECT * FROM t ORDER BY pk);
SELECT group_concat(msg,',') FROM (SELECT * FROM audit ORDER BY msg);
SELECT sql FROM sqlite_master WHERE name='tr_t_insert';
"

apply_bidirectional unchanged_trigger_side_effects "
CREATE TABLE z_audit(msg TEXT);
CREATE TABLE a_source(pk INTEGER PRIMARY KEY,v INTEGER);
CREATE TRIGGER tr_source_insert AFTER INSERT ON a_source BEGIN
  UPDATE a_source SET v=v+1 WHERE pk=new.pk;
  INSERT INTO z_audit VALUES('insert:'||new.pk);
END;
INSERT INTO a_source VALUES(1,10);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
INSERT INTO a_source VALUES(2,20);
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(pk||':'||v,',') FROM (SELECT * FROM a_source ORDER BY pk);
SELECT group_concat(msg,',') FROM (SELECT * FROM z_audit ORDER BY msg);
SELECT sql FROM sqlite_master WHERE name='tr_source_insert';
"

apply_bidirectional foreign_key_rebuild "
PRAGMA foreign_keys=ON;
CREATE TABLE parent(id INTEGER PRIMARY KEY,name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY,parent_id INTEGER);
INSERT INTO parent VALUES(1,'one'),(2,'two');
INSERT INTO child VALUES(10,1),(20,2);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
CREATE TABLE child_new(
  id INTEGER PRIMARY KEY,
  parent_id INTEGER NOT NULL REFERENCES parent(id) ON DELETE CASCADE
);
INSERT INTO child_new SELECT * FROM child;
DROP TABLE child;
ALTER TABLE child_new RENAME TO child;
UPDATE parent SET name='uno' WHERE id=1;
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
PRAGMA foreign_keys=ON;
SELECT group_concat(id||':'||name,',') FROM (SELECT * FROM parent ORDER BY id);
SELECT group_concat(id||':'||parent_id,',') FROM (SELECT * FROM child ORDER BY id);
SELECT count(*) FROM pragma_foreign_key_check;
SELECT sql FROM sqlite_master WHERE name='child';
"

apply_bidirectional rebuild_temp_name_collision "
CREATE TABLE __doltlite_patch_1(pk INTEGER PRIMARY KEY,v TEXT);
CREATE TABLE t(pk INTEGER PRIMARY KEY,v TEXT);
INSERT INTO __doltlite_patch_1 VALUES(1,'reserved');
INSERT INTO t VALUES(1,'one');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 7;
UPDATE t SET n=8 WHERE pk=1;
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(pk||':'||v,',') FROM __doltlite_patch_1;
SELECT group_concat(pk||':'||v||':'||n,',') FROM t;
SELECT group_concat(name,',') FROM (SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'dolt_%' ORDER BY name);
"

apply_bidirectional native_column_rename_with_view "
CREATE TABLE t(k INTEGER PRIMARY KEY,a TEXT);
CREATE VIEW v0 AS SELECT k FROM t;
CREATE INDEX ix_a ON t(a);
CREATE TRIGGER tr_a AFTER UPDATE OF a ON t BEGIN SELECT 1; END;
INSERT INTO t VALUES(1,'a1'),(2,'a2');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
ALTER TABLE t RENAME COLUMN a TO a2;
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT * FROM t ORDER BY k;
SELECT group_concat(type||':'||name||':'||sql,';') FROM (SELECT type,name,sql FROM sqlite_master WHERE tbl_name='t' OR name='v0' ORDER BY type,name);
PRAGMA integrity_check;
"

apply_forward native_column_add_with_view "
CREATE TABLE t(k INTEGER PRIMARY KEY,a TEXT);
CREATE VIEW v0 AS SELECT k FROM t;
CREATE INDEX ix_a ON t(a);
CREATE TRIGGER tr_a AFTER UPDATE OF a ON t BEGIN SELECT 1; END;
INSERT INTO t VALUES(1,'a1'),(2,'a2');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
ALTER TABLE t ADD COLUMN b TEXT DEFAULT 'x';
UPDATE t SET b='y' WHERE k=2;
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(k||':'||a||':'||b,',') FROM (SELECT * FROM t ORDER BY k);
SELECT group_concat(type||':'||name||':'||sql,';') FROM (SELECT type,name,sql FROM sqlite_master WHERE tbl_name='t' OR name='v0' ORDER BY type,name);
PRAGMA integrity_check;
"


# Views as DDL (Dolt uses dolt_schemas); creates after tables; bidirectional drop.
apply_bidirectional view_redefine_and_add "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE VIEW v_old AS SELECT id FROM t;
INSERT INTO t VALUES(1,'x');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
DROP VIEW v_old;
CREATE VIEW v_old AS SELECT id, v FROM t;
CREATE VIEW v_new AS SELECT count(*) AS n FROM t;
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(name||'='||replace(sql,' ',''),';') FROM (SELECT name, sql FROM sqlite_master WHERE type='view' ORDER BY name);
SELECT group_concat(id||':'||v,',') FROM (SELECT * FROM t ORDER BY id);
"

# Quoted table name with a paren must not split the CREATE body.
apply_bidirectional quoted_paren_table_name "
CREATE TABLE \"we (them)\"(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO \"we (them)\" VALUES(1,'x');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_tag('base');
ALTER TABLE \"we (them)\" ADD COLUMN w TEXT;
UPDATE \"we (them)\" SET w='y';
SELECT dolt_commit('-A','-m','target');
SELECT dolt_tag('target');
" "
SELECT group_concat(id||':'||v,',') FROM (SELECT id, v FROM \"we (them)\" ORDER BY id);
SELECT group_concat(name,',') FROM pragma_table_info('we (them)');
SELECT replace(sql,' ','') FROM sqlite_master WHERE name='we (them)';
"

echo ""
echo "=== dolt_patch oracle results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
