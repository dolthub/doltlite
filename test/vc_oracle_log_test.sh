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
      h = $1
      if (!(h in seen)) { n++; seen[h] = "H" n }
      $1 = seen[h]
      print $1 "\t" $2
    }
  '
}

oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_q="SELECT 'LOG|' || commit_hash || char(9) || message FROM dolt_log"
  local dt_q="SELECT concat('LOG|', commit_hash, char(9), message) FROM dolt_log ORDER BY commit_order DESC"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$dl_q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^LOG|' \
           | sed 's/^LOG|//' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$dt_q"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(tr -d '"' < "$dir/dt.raw" | grep '^LOG|' | sed 's/^LOG|//' | normalize)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

normalize_commit_relations() {
  tr -d '"\r' | awk -F'|' '
    function sym(h) {
      if (!(h in seen)) { n++; seen[h] = "H" n }
      return seen[h]
    }
    function is_hash(s) {
      return s ~ /^[0-9a-f]{40}$/ || s ~ /^[0-9a-v]{32}$/
    }
    {
      for (i=1; i<=NF; i++) {
        if (is_hash($i)) $i = sym($i)
      }
      out = $1
      for (i=2; i<=NF; i++) out = out "|" $i
      print out
    }
  '
}

oracle_commit_relations() {
  local name="$1" dl_sql="$2" dt_sql="$3"
  local dir="$TMPROOT/${name}_rel"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(
    printf ".headers off\n.mode list\n%s\n" "$dl_sql" \
      | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
      | grep -E '^(C[0-9]+|H[0-9]+|L[0-9]+|LOG)[|]' \
      | normalize_commit_relations
  )

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf '%s\n' "$dt_sql" \
      | "$DOLT" sql -c -r csv 2>"$dir/dt.err" \
      | tr -d '"\r' \
      | awk '
          function is_dolt_hash(s) { return s ~ /^[0-9a-v]{32}$/ }
          /^hash$/ { want_commit=1; next }
          want_commit && is_dolt_hash($0) {
            c++; print "C" c "|" $0; want_commit=0; next
          }
          want_commit {
            c++; print "C" c "|0"; want_commit=0
          }
          /^(H[0-9]+|L[0-9]+|LOG)\|/ { print }
        ' \
      | normalize_commit_relations
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_commit_error_poststate() {
  local name="$1" dl_setup="$2" dl_call="$3" dl_query="$4" dt_setup="$5" dt_call="$6" dt_query="$7"
  local dir="$TMPROOT/${name}_errstate"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  printf ".headers off\n.mode list\n%s\n%s\n" "$dl_setup" "$dl_call" \
    | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.out" 2>"$dir/dl.err"
  dl_rc=$?
  local dl_out
  dl_out=$(
    printf ".headers off\n.mode list\n%s\n" "$dl_query" \
      | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
      | grep -E '^(H|L|LOG)[|]' \
      | normalize_commit_relations
  )

  local dt_rc
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf '%s\n%s\n' "$dt_setup" "$dt_call" \
      | "$DOLT" sql -r csv >"$dir/dt.out" 2>"$dir/dt.err"
  )
  dt_rc=$?
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    printf '%s\n' "$dt_query" \
      | "$DOLT" sql -c -r csv 2>>"$dir/dt.err" \
      | tr -d '"\r' \
      | grep -E '^(H|L|LOG)[|]' \
      | normalize_commit_relations
  )

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc" && [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

echo "=== Version Control Oracle Tests: dolt_log ==="
echo ""

echo "--- fresh repo ---"

oracle "fresh_db_has_seed_commit" "
-- no user commits; both sides should report a single seed commit
SELECT 1;
"

echo "--- linear chains ---"

oracle "single_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'first');
"

oracle "three_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'c2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'c3');
"

oracle "commit_all_flag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
INSERT INTO t VALUES (2, 20);
SELECT dolt_commit('-a', '-m', 'second');
"

echo "--- no-diff commit return/head/log consistency ---"

oracle_commit_relations "allow_empty_second_commit_advances_head" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT 'C1|' || dolt_commit('-A', '-m', 'first');
SELECT 'H1|' || dolt_hashof('HEAD');
SELECT 'L1|' || count(*) FROM dolt_log;
SELECT 'C2|' || dolt_commit('--allow-empty', '-m', 'second');
SELECT 'H2|' || dolt_hashof('HEAD');
SELECT 'L2|' || count(*) FROM dolt_log;
SELECT 'LOG|' || commit_hash || '|' || message FROM dolt_log;
" "
CREATE TABLE t(id int primary key, v text);
INSERT INTO t VALUES (1, 'a');
CALL dolt_commit('-A', '-m', 'first');
SELECT concat('H1|', dolt_hashof('HEAD'));
SELECT concat('L1|', count(*)) FROM dolt_log;
CALL dolt_commit('--allow-empty', '-m', 'second');
SELECT concat('H2|', dolt_hashof('HEAD'));
SELECT concat('L2|', count(*)) FROM dolt_log;
SELECT concat('LOG|', commit_hash, '|', message) FROM dolt_log ORDER BY commit_order DESC;
"

oracle_commit_relations "skip_empty_second_commit_returns_zero_without_advancing" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT 'C1|' || dolt_commit('-A', '-m', 'first');
SELECT 'H1|' || dolt_hashof('HEAD');
SELECT 'L1|' || count(*) FROM dolt_log;
SELECT 'C2|' || dolt_commit('--skip-empty', '-m', 'second');
SELECT 'H2|' || dolt_hashof('HEAD');
SELECT 'L2|' || count(*) FROM dolt_log;
SELECT 'LOG|' || commit_hash || '|' || message FROM dolt_log;
" "
CREATE TABLE t(id int primary key, v text);
INSERT INTO t VALUES (1, 'a');
CALL dolt_commit('-A', '-m', 'first');
SELECT concat('H1|', dolt_hashof('HEAD'));
SELECT concat('L1|', count(*)) FROM dolt_log;
CALL dolt_commit('--skip-empty', '-m', 'second');
SELECT concat('H2|', dolt_hashof('HEAD'));
SELECT concat('L2|', count(*)) FROM dolt_log;
SELECT concat('LOG|', commit_hash, '|', message) FROM dolt_log ORDER BY commit_order DESC;
"

oracle_commit_error_poststate "clean_second_commit_errors_without_advancing" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_commit('-A', '-m', 'first');
" "SELECT dolt_commit('-A', '-m', 'second');" "
SELECT 'H|' || dolt_hashof('HEAD');
SELECT 'L|' || count(*) FROM dolt_log;
SELECT 'LOG|' || commit_hash || '|' || message FROM dolt_log;
" "
CREATE TABLE t(id int primary key, v text);
INSERT INTO t VALUES (1, 'a');
CALL dolt_commit('-A', '-m', 'first');
" "CALL dolt_commit('-A', '-m', 'second');" "
SELECT concat('H|', dolt_hashof('HEAD'));
SELECT concat('L|', count(*)) FROM dolt_log;
SELECT concat('LOG|', commit_hash, '|', message) FROM dolt_log ORDER BY commit_order DESC;
"

oracle "message_with_special_chars" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'fix: handle x<y & z>w, OK?');
"

oracle "amend_like_via_reset" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'one');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'two');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'three');
"

echo "--- message edge cases ---"

oracle "unicode_message" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'fix: données à jour 日本語 🚀');
"

oracle "very_long_message" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'this is a deliberately very long commit message that goes on and on and on to exercise any buffer-size assumptions in the log walker or in either engine|s output format and should still come back intact');
"

oracle "internal_whitespace_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'one    two     three');
"

oracle "leading_trailing_whitespace_trimmed" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', '   hello world   ');
"

echo "--- merge commits ---"

oracle "merge_commit_in_log" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
"

oracle "merge_then_more_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'post_merge');
"

echo "--- tags and branches ---"

oracle "tag_does_not_add_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_tag('v1');
"

oracle "log_on_feature_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
"

oracle "log_on_branch_created_from_tag_ref" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_tag('v1', 'HEAD~1');
SELECT dolt_branch('from_tag', 'v1');
SELECT dolt_checkout('from_tag');
"

oracle "log_on_branch_created_from_first_parent" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feat');
SELECT dolt_branch('from_p1', 'HEAD^1');
SELECT dolt_checkout('from_p1');
"

oracle "log_on_branch_created_from_second_parent" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feat');
SELECT dolt_branch('from_p2', 'HEAD^2');
SELECT dolt_checkout('from_p2');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
