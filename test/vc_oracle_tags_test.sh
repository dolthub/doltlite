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
  tr -d '\r' | awk -F'\t' '
    {
      h = $2
      if (!(h in seen)) { n++; seen[h] = "H" n }
      $2 = seen[h]
      out = $1
      for (i = 2; i <= NF; i++) out = out "\t" $i
      print out
    }
  '
}

oracle() {
  local name="$1" setup="$2" allow_empty="${3:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local q='SELECT tag_name || char(9) || tag_hash || char(9) || message FROM dolt_tags ORDER BY tag_name'

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat(tag_name, char(9), tag_hash, char(9), message) FROM dolt_tags ORDER BY tag_name;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(vc_oracle_tail_csv_body "$dir/dt.raw" | normalize)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

oracle_savepoint_tag_poststate() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_sp"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc dt_rc dl_v dl_tags dt_v dt_tags

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  dl_v=$(printf ".headers off\n.mode list\nSELECT v FROM t WHERE id=1;\n" \
         | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err")
  dl_tags=$(printf ".headers off\n.mode list\nSELECT coalesce(group_concat(tag_name, ','), '') FROM dolt_tags;\n" \
            | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err")

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?
  dt_v=$(cd "$dir/dt" && "$DOLT" sql -r csv -q "SELECT v FROM t WHERE id=1;" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"')
  dt_tags=$(cd "$dir/dt" && "$DOLT" sql -r csv -q "SELECT coalesce(group_concat(tag_name, ','), '') FROM dolt_tags;" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"')

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc" && [ "$dl_v" = "$dt_v" ] && [ "$dl_tags" = "$dt_tags" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc/v/tags:"; { echo "$dl_rc"; echo "$dl_v"; echo "$dl_tags"; } | sed 's/^/      /'
    echo "    dolt rc/v/tags:"; { echo "$dt_rc"; echo "$dt_v"; echo "$dt_tags"; } | sed 's/^/      /'
  fi
}

echo "=== Version Control Oracle Tests: dolt_tags ==="
echo ""

echo "--- baseline ---"

oracle "no_tags_on_fresh_repo" "
SELECT 1;
" "EXPECT_EMPTY"

echo "--- single tag ---"

oracle "tag_head_no_message" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_tag('v1.0');
"

oracle "tag_head_with_message" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_tag('v1.0', '-m', 'first release');
"

oracle "tag_message_with_special_chars" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_tag('v1.0', '-m', 'fix: x<y & z>w, ok?');
"

echo "--- multiple tags ---"

oracle "two_tags_on_same_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_tag('v1.0');
SELECT dolt_tag('latest');
"

oracle "tags_on_different_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_tag('v1.0');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_tag('v2.0');
"

echo "--- tagging older commits ---"

oracle "tag_older_commit_by_hash" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES (2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_tag('historical', (SELECT commit_hash FROM dolt_log WHERE message='c1'));
"

oracle "tag_parent_ref_head_parent" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES (2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_tag('parent', 'HEAD^1');
"

oracle "tag_parent_ref_head_tilde" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES (2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_tag('parenttilde', 'HEAD~1');
"

oracle "tag_branch_ref" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
SELECT dolt_tag('feat-tag', 'feat');
"

echo "--- deletion ---"

oracle "delete_tag" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_tag('temp');
SELECT dolt_tag('-d', 'temp');
" "EXPECT_EMPTY"

oracle "delete_one_keep_others" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_tag('keep');
SELECT dolt_tag('drop');
SELECT dolt_tag('-d', 'drop');
"

oracle "delete_and_recreate_same_name" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_tag('temp');
SELECT dolt_tag('-d', 'temp');
SELECT dolt_tag('temp');
"

echo "--- savepoint parity ---"

oracle_savepoint_tag_poststate "tag_inside_savepoint_releases_savepoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SAVEPOINT sp1;
UPDATE t SET v='dirty' WHERE id=1;
SELECT dolt_tag('v1');
ROLLBACK TO sp1;
"

oracle_savepoint_tag_poststate "tag_delete_missing_inside_savepoint_invalidates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SAVEPOINT sp1;
SELECT dolt_tag('-d', 'missing');
ROLLBACK TO sp1;
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
