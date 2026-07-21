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

normalize() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 2 && $1 == "A" { print }' \
    | sort
}

oracle() {
  local name="$1" setup="$2" ref="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_q="SELECT 'A' || char(9) || coalesce(id,'') || char(9) || coalesce(v,'') FROM dolt_at_t WHERE commit_ref = '${ref}' ORDER BY id"
  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$dl_q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_q="SELECT concat('A', char(9), coalesce(id,''), char(9), coalesce(v,'')) FROM t AS OF '${ref}' ORDER BY id"

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$dt_q"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" | tr -d '"' | normalize
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_error() {
  local name="$1" setup="$2" ref="$3"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_sql
  local dl_rc
  dl_sql=$(printf "%s\nSELECT * FROM dolt_at_t WHERE commit_ref = '%s';\n" "$setup" "$ref")
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$dl_sql"
  dl_rc=$?

  local dolt_setup
  local dt_sql
  local dt_rc
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  dt_sql=$(printf "%s\nSELECT * FROM t AS OF '%s';\n" "$dolt_setup" "$ref")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dt_sql"
  dt_rc=$?

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
  fi
}

oracle_table_after_reopen() {
  local name="$1" setup="$2" table="$3" ref="$4"
  local dl_q="SELECT 'A' || char(9) || coalesce(id,'') || char(9) || coalesce(v,'') FROM dolt_at_${table} WHERE commit_ref = '${ref}' ORDER BY id"
  local dt_q="SELECT concat('A', char(9), coalesce(id,''), char(9), coalesce(v,'')) FROM ${table} AS OF '${ref}' ORDER BY id"
  oracle_query_after_reopen "$name" "$setup" "$dl_q" "$dt_q"
}

oracle_query_after_reopen() {
  local name="$1" setup="$2" dl_q="$3" dt_q="$4"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.setup.out" \
    "$dir/dl.setup.err" "$setup"

  local dl_out
  dl_out=$(printf ".headers off\n.mode list\n.separator '\t'\n%s;\n" "$dl_q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$dt_q"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" | tr -d '"' | normalize
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: dolt_at_<table> ==="
echo ""

SEED="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- HEAD ref ---"

oracle "at_head_two_rows" "$SEED" "HEAD"

oracle "at_head_after_second_commit" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD"

echo "--- HEAD~N ref ---"

oracle "at_head_minus_1_after_modify" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_modify');
" "HEAD~1"

oracle "at_head_minus_2" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3');
" "HEAD~2"

oracle "at_head_tilde_no_number" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~"

echo "--- branch ref ---"

oracle "at_branch_main" "$SEED" "main"

oracle "at_sibling_branch_feature" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
" "feature"

oracle_table_after_reopen "at_table_only_on_sibling_branch" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
CREATE TABLE feature_only(id INT PRIMARY KEY, v TEXT);
INSERT INTO feature_only VALUES (1, 'feature-one');
INSERT INTO feature_only VALUES (2, 'feature-two');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feature table');
SELECT dolt_checkout('main');
" "feature_only" "feature"

echo "--- branch-only table schema shapes after reopen ---"

oracle_query_after_reopen "at_branch_only_text_pk" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
CREATE TABLE text_pk_only(code VARCHAR(32) PRIMARY KEY, v TEXT);
INSERT INTO text_pk_only VALUES ('b', 'text-two');
INSERT INTO text_pk_only VALUES ('a', 'text-one');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'text pk table');
SELECT dolt_checkout('main');
" \
"SELECT 'A' || char(9) || coalesce(code,'') || char(9) || coalesce(v,'') FROM dolt_at_text_pk_only WHERE commit_ref = 'feature' ORDER BY code" \
"SELECT concat('A', char(9), coalesce(code,''), char(9), coalesce(v,'')) FROM text_pk_only AS OF 'feature' ORDER BY code"

oracle_query_after_reopen "at_branch_only_composite_pk" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
CREATE TABLE comp_only(org VARCHAR(32), id INT, v TEXT, PRIMARY KEY(org, id));
INSERT INTO comp_only VALUES ('org-b', 2, 'comp-two');
INSERT INTO comp_only VALUES ('org-a', 1, 'comp-one');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'composite pk table');
SELECT dolt_checkout('main');
" \
"SELECT 'A' || char(9) || coalesce(org,'') || char(9) || coalesce(id,'') || char(9) || coalesce(v,'') FROM dolt_at_comp_only WHERE commit_ref = 'feature' ORDER BY org, id" \
"SELECT concat('A', char(9), coalesce(org,''), char(9), coalesce(id,''), char(9), coalesce(v,'')) FROM comp_only AS OF 'feature' ORDER BY org, id"

oracle_query_after_reopen "at_branch_only_extra_columns" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
CREATE TABLE extra_only(id INT PRIMARY KEY, v TEXT, n INT, note TEXT);
INSERT INTO extra_only VALUES (1, 'extra-one', 42, NULL);
INSERT INTO extra_only VALUES (2, 'extra-two', NULL, 'has-note');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'extra column table');
SELECT dolt_checkout('main');
" \
"SELECT 'A' || char(9) || coalesce(id,'') || char(9) || coalesce(v,'') || char(9) || CASE WHEN n IS NULL THEN 'NULL' ELSE CAST(n AS CHAR) END || char(9) || coalesce(note,'NULL') FROM dolt_at_extra_only WHERE commit_ref = 'feature' ORDER BY id" \
"SELECT concat('A', char(9), coalesce(id,''), char(9), coalesce(v,''), char(9), CASE WHEN n IS NULL THEN 'NULL' ELSE CAST(n AS CHAR) END, char(9), coalesce(note,'NULL')) FROM extra_only AS OF 'feature' ORDER BY id"

oracle_query_after_reopen "at_branch_schema_changed_common_columns" "
$SEED
CREATE TABLE changing(id INT PRIMARY KEY, v TEXT);
INSERT INTO changing VALUES (1, 'main-one');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'changing base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
ALTER TABLE changing ADD COLUMN extra TEXT;
UPDATE changing SET v = 'feature-one', extra = 'feature-extra' WHERE id = 1;
INSERT INTO changing VALUES (2, 'feature-two', 'feature-extra-two');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'changing feature schema');
SELECT dolt_checkout('main');
" \
"SELECT 'A' || char(9) || coalesce(id,'') || char(9) || coalesce(v,'') FROM dolt_at_changing WHERE commit_ref = 'feature' ORDER BY id" \
"SELECT concat('A', char(9), coalesce(id,''), char(9), coalesce(v,'')) FROM changing AS OF 'feature' ORDER BY id"

oracle_query_after_reopen "at_branch_only_quoted_table_name" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
CREATE TABLE \`feature only\`(id INT PRIMARY KEY, v TEXT);
INSERT INTO \`feature only\` VALUES (1, 'quoted-one');
INSERT INTO \`feature only\` VALUES (2, 'quoted-two');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'quoted table');
SELECT dolt_checkout('main');
" \
"SELECT 'A' || char(9) || coalesce(id,'') || char(9) || coalesce(v,'') FROM \"dolt_at_feature only\" WHERE commit_ref = 'feature' ORDER BY id" \
"SELECT concat('A', char(9), coalesce(id,''), char(9), coalesce(v,'')) FROM \`feature only\` AS OF 'feature' ORDER BY id"

echo "--- tag ref ---"

oracle "at_tag" "
$SEED
SELECT dolt_tag('v1');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "v1"

oracle "at_branch_created_from_tag" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_tag('v1', 'HEAD~1');
SELECT dolt_branch('from_tag', 'v1');
" "from_tag"

oracle_table_after_reopen "at_table_only_at_tag" "
$SEED
CREATE TABLE tagged_only(id INT PRIMARY KEY, v TEXT);
INSERT INTO tagged_only VALUES (1, 'tagged-one');
INSERT INTO tagged_only VALUES (2, 'tagged-two');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'tagged table');
SELECT dolt_tag('has_tagged_only');
DROP TABLE tagged_only;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop tagged table');
" "tagged_only" "has_tagged_only"

echo "--- bare commit hash ref ---"

oracle "at_recent_commit_via_head" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD"

oracle "at_raw_hash_head_minus_1" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1"

oracle_table_after_reopen "at_dropped_table_at_head_minus_1" "
$SEED
CREATE TABLE dropped_only(id INT PRIMARY KEY, v TEXT);
INSERT INTO dropped_only VALUES (1, 'dropped-one');
INSERT INTO dropped_only VALUES (2, 'dropped-two');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'dropped table');
DROP TABLE dropped_only;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop dropped table');
" "dropped_only" "HEAD~1"

echo "--- working set is NOT visible at any ref ---"

oracle "at_head_excludes_working_modifications" "
$SEED
UPDATE t SET v = 999 WHERE id = 1;
" "HEAD"

oracle "at_head_excludes_staged_modifications" "
$SEED
UPDATE t SET v = 999 WHERE id = 1;
SELECT dolt_add('-A');
" "HEAD"

echo "--- post-merge ---"

oracle "at_head_after_merge" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
" "HEAD"

oracle "at_head_minus_1_after_merge_is_main2" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
" "HEAD~1"

oracle "at_head_first_parent_after_merge" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
" "HEAD^1"

oracle "at_head_second_parent_after_merge" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
" "HEAD^2"

echo "--- error paths ---"

oracle_error "at_nonexistent_ref" "$SEED" "nope"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
