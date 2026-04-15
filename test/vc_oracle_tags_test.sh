#!/bin/bash
#
# Version-control oracle test: dolt_tags
#
# Runs identical tag-management scenarios against doltlite and Dolt and
# compares the normalized dolt_tags output. Catches divergence in how each
# engine reports tag listings, the per-tag tagger metadata and message,
# and which commit a tag points at.
#
# Columns compared: tag_name, tag_hash (normalized), message. The
# tagger/email/date columns are excluded because their values come from
# process state and legitimately differ across the two engines unless
# every scenario passes --author overrides; the message and pointed-at
# commit are the load-bearing semantic axes.
#
# Usage: bash vc_oracle_tags_test.sh [path/to/doltlite] [path/to/dolt]
#

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
  local name="$1" setup="$2"
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
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat(tag_name, char(9), tag_hash, char(9), message) FROM dolt_tags ORDER BY tag_name;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(vc_oracle_tail_csv_body "$dir/dt.raw" | normalize)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:"    ; echo "$dt_out" | sed 's/^/      /'
  fi
}

echo "=== Version Control Oracle Tests: dolt_tags ==="
echo ""

echo "--- baseline ---"

oracle "no_tags_on_fresh_repo" "
SELECT 1;
"

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

echo "--- deletion ---"

oracle "delete_tag" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_tag('temp');
SELECT dolt_tag('-d', 'temp');
"

oracle "delete_one_keep_others" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_tag('keep');
SELECT dolt_tag('drop');
SELECT dolt_tag('-d', 'drop');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
