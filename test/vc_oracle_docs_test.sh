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

# Default AGENT.md text differs; filter it unless the test overwrote the row.
normalize_docs() { tr -d '\r"' | grep '^D|' | grep -v '^D|AGENT\.md|' | sort; }
normalize_status() { tr -d '\r"' | grep '^S|' | sort; }
normalize_agent() { tr -d '\r"' | grep '^A|' | sort; }
normalize_diff_stat() { tr -d '\r"' | grep '^V|' | sort; }

DOCS_QUERY_DL="SELECT 'D|' || doc_name || '|' || doc_text FROM dolt_docs;"
DOCS_QUERY_DT="SELECT concat('D|', doc_name, '|', doc_text) FROM dolt_docs;"
STATUS_QUERY_DL="SELECT 'S|' || table_name || '|' || staged || '|' || status FROM dolt_status;"
STATUS_QUERY_DT="SELECT concat('S|', table_name, '|', staged, '|', status) FROM dolt_status;"
AGENT_QUERY_DL="SELECT 'A|' || doc_name || '|' || doc_text FROM dolt_docs WHERE doc_name = 'AGENT.md';"
AGENT_QUERY_DT="SELECT concat('A|', doc_name, '|', doc_text) FROM dolt_docs WHERE doc_name = 'AGENT.md';"
AGENT_COUNT_DL="SELECT 'A|' || count(*) FROM dolt_docs WHERE doc_name = 'AGENT.md';"
AGENT_COUNT_DT="SELECT concat('A|', count(*)) FROM dolt_docs WHERE doc_name = 'AGENT.md';"
DIFF_STAT_DL="SELECT 'V|' || rows_added || '|' || rows_deleted || '|' || rows_modified || '|' || old_row_count || '|' || new_row_count FROM dolt_diff_stat('HEAD', 'WORKING', 'dolt_docs');"
DIFF_STAT_DT="SELECT concat('V|', rows_added, '|', rows_deleted, '|', rows_modified, '|', old_row_count, '|', new_row_count) FROM dolt_diff_stat('HEAD', 'WORKING', 'dolt_docs');"

oracle() {
  local kind="$1" name="$2" setup="$3" allow_empty="${4:-}"
  local dir="$TMPROOT/$name"
  local dl_query dt_query norm
  if [ "$kind" = "docs" ]; then
    dl_query="$DOCS_QUERY_DL"; dt_query="$DOCS_QUERY_DT"; norm=normalize_docs
  elif [ "$kind" = "agent" ]; then
    dl_query="$AGENT_QUERY_DL"; dt_query="$AGENT_QUERY_DT"; norm=normalize_agent
  elif [ "$kind" = "agent_count" ]; then
    dl_query="$AGENT_COUNT_DL"; dt_query="$AGENT_COUNT_DT"; norm=normalize_agent
  elif [ "$kind" = "diff_stat" ]; then
    dl_query="$DIFF_STAT_DL"; dt_query="$DIFF_STAT_DT"; norm=normalize_diff_stat
  else
    dl_query="$STATUS_QUERY_DL"; dt_query="$STATUS_QUERY_DT"; norm=normalize_status
  fi
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$dl_query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | $norm)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf "%s\n%s\n" "$dolt_setup" "$dt_query" \
      | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$($norm < "$dir/dt.raw")

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

oracle_docs() { oracle docs "$@"; }
oracle_status() { oracle status "$@"; }
oracle_agent() { oracle agent "$@"; }
oracle_agent_count() { oracle agent_count "$@"; }
oracle_diff_stat() { oracle diff_stat "$@"; }

oracle_divergence() {
  local name="$1" setup="$2" dl_query="$3" dt_query="$4"
  local dl_expected="$5" dt_expected="$6"
  local dl_expect_error="$7" dt_expect_error="$8"
  local dir="$TMPROOT/${name}_div"
  local dl_rc dt_rc dl_out dt_out dl_rc_ok=0 dt_rc_ok=0
  mkdir -p "$dir/dl" "$dir/dt"

  printf "%s\n%s\n" "$setup" "$dl_query" \
    | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.raw" 2>"$dir/dl.err"
  dl_rc=$?
  dl_out=$(tr -d '\r"' < "$dir/dl.raw" | grep '^X|')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf "%s\n%s\n" "$dolt_setup" "$dt_query" \
      | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_rc=$?
  dt_out=$(tr -d '\r"' < "$dir/dt.raw" | grep '^X|')

  if [ "$dl_expect_error" = "1" ]; then
    vc_oracle_is_clean_error "$dl_rc" && dl_rc_ok=1
  elif [ "$dl_rc" -eq 0 ]; then
    dl_rc_ok=1
  fi
  if [ "$dt_expect_error" = "1" ]; then
    vc_oracle_is_clean_error "$dt_rc" && dt_rc_ok=1
  elif [ "$dt_rc" -eq 0 ]; then
    dt_rc_ok=1
  fi

  if [ "$dl_rc_ok" -eq 1 ] && [ "$dt_rc_ok" -eq 1 ] \
     && [ "$dl_out" = "$dl_expected" ] \
     && [ "$dt_out" = "$dt_expected" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc/output: $dl_rc / $dl_out"
    echo "    expected:           $dl_expect_error / $dl_expected"
    echo "    dolt rc/output:     $dt_rc / $dt_out"
    echo "    expected:           $dt_expect_error / $dt_expected"
  fi
}

oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
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

echo "=== Version Control Oracle Tests: dolt_docs ==="
echo ""

echo "--- reads without a backing table ---"

oracle_docs "fresh_select_empty" "
CREATE TABLE t(x INT PRIMARY KEY);
" "EXPECT_EMPTY"

oracle_status "fresh_status_empty" "
SELECT * FROM dolt_docs;
" "EXPECT_EMPTY"

echo "--- writes materialize the table ---"

oracle_docs "insert_without_create" "
INSERT INTO dolt_docs VALUES ('README.md', '# my project');
INSERT INTO dolt_docs VALUES ('LICENSE.md', 'MIT License');
"

oracle_status "status_new_table_after_insert" "
INSERT INTO dolt_docs VALUES ('README.md', '# my project');
"

oracle_status "status_new_table_zero_row_delete" "
DELETE FROM dolt_docs WHERE doc_name = 'nope';
"

oracle_status "status_new_table_zero_row_update" "
UPDATE dolt_docs SET doc_text = 'x' WHERE doc_name = 'nope';
"

echo "--- row operations ---"

oracle_docs "update_doc" "
INSERT INTO dolt_docs VALUES ('README.md', 'v1');
UPDATE dolt_docs SET doc_text = 'v2' WHERE doc_name = 'README.md';
"

oracle_docs "delete_doc" "
INSERT INTO dolt_docs VALUES ('README.md', 'v1');
INSERT INTO dolt_docs VALUES ('LICENSE.md', 'MIT');
DELETE FROM dolt_docs WHERE doc_name = 'README.md';
"

oracle_docs "delete_all_then_select" "
INSERT INTO dolt_docs VALUES ('README.md', 'v1');
DELETE FROM dolt_docs;
" "EXPECT_EMPTY"

oracle_docs "replace_upsert" "
INSERT INTO dolt_docs VALUES ('README.md', 'v1');
REPLACE INTO dolt_docs VALUES ('README.md', 'v2');
REPLACE INTO dolt_docs VALUES ('LICENSE.md', 'MIT');
"

oracle_docs "arbitrary_doc_names" "
INSERT INTO dolt_docs VALUES ('notes.txt', 'free-form names are allowed');
INSERT INTO dolt_docs VALUES ('no-extension', 'also fine');
"

oracle_docs "case_sensitive_doc_name_values" "
INSERT INTO dolt_docs VALUES ('README.md', 'upper');
INSERT INTO dolt_docs VALUES ('readme.md', 'lower');
"

oracle_docs "case_insensitive_table_name" "
INSERT INTO DOLT_DOCS VALUES ('README.md', 'v1');
UPDATE Dolt_Docs SET doc_text = 'v2';
"

echo "--- constraints ---"

oracle_error "dup_pk_rejected" "
INSERT INTO dolt_docs VALUES ('README.md', 'v1');
INSERT INTO dolt_docs VALUES ('README.md', 'v2');
"

oracle_error "null_doc_text_rejected" "
INSERT INTO dolt_docs VALUES ('README.md', NULL);
"

oracle_error "null_doc_name_rejected" "
INSERT INTO dolt_docs VALUES (NULL, 'text');
"

oracle_error "drop_without_backing_table" "
DROP TABLE dolt_docs;
"

echo "--- version control ---"

oracle_status "status_clean_after_commit" "
INSERT INTO dolt_docs VALUES ('README.md', 'v1');
SELECT dolt_add('dolt_docs');
SELECT dolt_commit('-m', 'add docs');
" "EXPECT_EMPTY"

oracle_status "status_staged_after_add" "
INSERT INTO dolt_docs VALUES ('README.md', 'v1');
SELECT dolt_add('-A');
"

oracle_status "status_modified_after_commit" "
INSERT INTO dolt_docs VALUES ('README.md', 'v1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add docs');
UPDATE dolt_docs SET doc_text = 'v2';
"

oracle_docs "branch_isolation" "
INSERT INTO dolt_docs VALUES ('README.md', 'main version');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add docs');
SELECT dolt_checkout('-b', 'feature');
UPDATE dolt_docs SET doc_text = 'feature version';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feature edit');
SELECT dolt_checkout('main');
"

oracle_docs "merge_ff_brings_docs" "
CREATE TABLE sentinel(x INT PRIMARY KEY);
SELECT dolt_commit('-A', '-m', 'base');
SELECT dolt_checkout('-b', 'feature');
INSERT INTO dolt_docs VALUES ('README.md', 'from feature');
SELECT dolt_commit('-A', '-m', 'feature adds docs');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle_docs "merge_three_way_no_conflict" "
INSERT INTO dolt_docs VALUES ('README.md', 'base');
SELECT dolt_commit('-A', '-m', 'base');
SELECT dolt_checkout('-b', 'b1');
INSERT INTO dolt_docs VALUES ('LICENSE.md', 'MIT');
SELECT dolt_commit('-A', '-m', 'b1 adds license');
SELECT dolt_checkout('main');
UPDATE dolt_docs SET doc_text = 'main edit' WHERE doc_name = 'README.md';
SELECT dolt_commit('-A', '-m', 'main edits readme');
SELECT dolt_merge('b1');
"

oracle_error "merge_conflicting_docs" "
INSERT INTO dolt_docs VALUES ('README.md', 'base');
SELECT dolt_commit('-A', '-m', 'base');
SELECT dolt_checkout('-b', 'b1');
UPDATE dolt_docs SET doc_text = 'b1 edit';
SELECT dolt_commit('-A', '-m', 'b1');
SELECT dolt_checkout('main');
UPDATE dolt_docs SET doc_text = 'main edit';
SELECT dolt_commit('-A', '-m', 'main');
SELECT dolt_merge('b1');
"

oracle_docs "reset_hard_restores_docs" "
INSERT INTO dolt_docs VALUES ('README.md', 'committed');
SELECT dolt_commit('-A', '-m', 'base');
UPDATE dolt_docs SET doc_text = 'dirty edit';
SELECT dolt_reset('--hard');
"

oracle_status "drop_committed_docs" "
INSERT INTO dolt_docs VALUES ('README.md', 'v1');
SELECT dolt_commit('-A', '-m', 'add docs');
DROP TABLE dolt_docs;
"

oracle_docs "select_after_drop" "
INSERT INTO dolt_docs VALUES ('README.md', 'v1');
SELECT dolt_commit('-A', '-m', 'add docs');
DROP TABLE dolt_docs;
" "EXPECT_EMPTY"

echo "--- default AGENT.md ---"

oracle_agent_count "agent_present_on_fresh_repo" "
SELECT 1;
"

oracle_agent_count "agent_survives_other_writes" "
INSERT INTO dolt_docs VALUES ('README.md', 'hi');
"

oracle_divergence "default_agent_is_versioned_in_doltlite" "
INSERT INTO dolt_docs VALUES ('README.md', 'hi');
" \
"SELECT 'X|' || rows_added || '|' || rows_deleted || '|' || rows_modified || '|' || old_row_count || '|' || new_row_count FROM dolt_diff_stat('HEAD', 'WORKING', 'dolt_docs');" \
"SELECT concat('X|', rows_added, '|', rows_deleted, '|', rows_modified, '|', old_row_count, '|', new_row_count) FROM dolt_diff_stat('HEAD', 'WORKING', 'dolt_docs');" \
"X|2|0|0|0|2" "X|1|0|0|0|1" 0 0

oracle_agent "agent_replace_overrides_default" "
REPLACE INTO dolt_docs VALUES ('AGENT.md', 'custom agent doc');
"

oracle_agent "agent_update_overrides_default" "
UPDATE dolt_docs SET doc_text = 'edited agent doc' WHERE doc_name = 'AGENT.md';
"

oracle_divergence "agent_insert_obeys_stored_primary_key" "
INSERT INTO dolt_docs VALUES ('AGENT.md', 'inserted agent doc');
" \
"SELECT 'X|' || count(*) || '|' || coalesce(max(CASE WHEN doc_text='inserted agent doc' THEN 1 ELSE 0 END), 0) FROM dolt_docs WHERE doc_name='AGENT.md';" \
"SELECT concat('X|', count(*), '|', coalesce(max(CASE WHEN doc_text='inserted agent doc' THEN 1 ELSE 0 END), 0)) FROM dolt_docs WHERE doc_name='AGENT.md';" \
"X|1|0" "X|1|1" 1 0

oracle_divergence "agent_delete_sticks_in_doltlite" "
REPLACE INTO dolt_docs VALUES ('AGENT.md', 'custom agent doc');
DELETE FROM dolt_docs WHERE doc_name = 'AGENT.md';
" \
"SELECT 'X|' || count(*) FROM dolt_docs WHERE doc_name='AGENT.md';" \
"SELECT concat('X|', count(*)) FROM dolt_docs WHERE doc_name='AGENT.md';" \
"X|0" "X|1" 0 0

oracle_agent "agent_override_commits" "
REPLACE INTO dolt_docs VALUES ('AGENT.md', 'committed agent doc');
SELECT dolt_commit('-A', '-m', 'agent doc');
"

echo "--- large content round-trip ---"

BIGDOC=$(printf 'lorem-ipsum-%.0s' $(seq 1 500))
oracle_docs "large_doc_text" "
INSERT INTO dolt_docs VALUES ('README.md', '$BIGDOC');
"

echo "--- doltlite-only: CREATE TABLE shape guard ---"

doltlite_schema_reject() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/${name}_rej"
  mkdir -p "$dir/dl"
  echo "$sql" | "$DOLTLITE" "$dir/dl/db" > "$dir/out" 2>&1
  if grep -qi 'dolt_docs' "$dir/out" \
     && grep -qiE 'error|fail' "$dir/out"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected doltlite to reject)"
    echo "    output:"; sed 's/^/      /' "$dir/out"
  fi
}

doltlite_schema_accept() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/${name}_acc"
  mkdir -p "$dir/dl"
  echo "$sql" | "$DOLTLITE" "$dir/dl/db" > "$dir/out" 2>&1
  if ! grep -qiE 'error|fail' "$dir/out"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected doltlite to accept)"
    echo "    output:"; sed 's/^/      /' "$dir/out"
  fi
}

doltlite_schema_reject "schema_one_column" "
CREATE TABLE dolt_docs(doc_name TEXT NOT NULL PRIMARY KEY);
"

doltlite_schema_reject "schema_three_columns" "
CREATE TABLE dolt_docs(doc_name TEXT NOT NULL, doc_text TEXT NOT NULL, extra INT, PRIMARY KEY(doc_name));
"

doltlite_schema_reject "schema_wrong_first_name" "
CREATE TABLE dolt_docs(name TEXT NOT NULL, doc_text TEXT NOT NULL, PRIMARY KEY(name));
"

doltlite_schema_reject "schema_wrong_second_name" "
CREATE TABLE dolt_docs(doc_name TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(doc_name));
"

doltlite_schema_reject "schema_wrong_name_type" "
CREATE TABLE dolt_docs(doc_name INTEGER NOT NULL, doc_text TEXT NOT NULL, PRIMARY KEY(doc_name));
"

doltlite_schema_reject "schema_name_nullable" "
CREATE TABLE dolt_docs(doc_name TEXT, doc_text TEXT NOT NULL, PRIMARY KEY(doc_name));
"

doltlite_schema_reject "schema_text_nullable" "
CREATE TABLE dolt_docs(doc_name TEXT NOT NULL, doc_text TEXT, PRIMARY KEY(doc_name));
"

doltlite_schema_reject "schema_compound_pk" "
CREATE TABLE dolt_docs(doc_name TEXT NOT NULL, doc_text TEXT NOT NULL, PRIMARY KEY(doc_name, doc_text));
"

doltlite_schema_reject "schema_no_pk" "
CREATE TABLE dolt_docs(doc_name TEXT NOT NULL, doc_text TEXT NOT NULL);
"

doltlite_schema_accept "schema_exact" "
CREATE TABLE dolt_docs(doc_name TEXT NOT NULL, doc_text TEXT NOT NULL, PRIMARY KEY(doc_name));
INSERT INTO dolt_docs VALUES ('README.md', 'hand-created');
SELECT * FROM dolt_docs;
"

doltlite_schema_accept "schema_varchar_longtext" "
CREATE TABLE dolt_docs(doc_name VARCHAR(1023) NOT NULL, doc_text LONGTEXT NOT NULL, PRIMARY KEY(doc_name));
INSERT INTO dolt_docs VALUES ('README.md', 'hand-created');
SELECT * FROM dolt_docs;
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
