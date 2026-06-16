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

normalize_log() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 5 && $1 == "L" { print }' \
    | awk -F'\t' '
        {
          email = $4
          if (email == "" \
           || email == "root@localhost" \
           || email == "oracle@test" \
           || email == "noreply@dolthub.com" \
           || email == "doltlite@local") {
            email = "DEFAULT"
          }
          dt = substr($5, 1, 10)
          "date +%Y-%m-%d" | getline today
          close("date +%Y-%m-%d")
          "date -u +%Y-%m-%d" | getline utc_today
          close("date -u +%Y-%m-%d")
          if (dt == today || dt == utc_today) {
            dt = "RECENT"
          }
          print "L\t" $2 "\t" $3 "\t" email "\t" dt
        }
      ' \
    | sort -t$'\t' -k3 \
    | awk -F'\t' '
        {
          h = $2
          if (!(h in seen)) { n++; seen[h] = "H" n }
          print "L\t" seen[h] "\t" $3 "\t" $4 "\t" $5
        }
      '
}

normalize_status() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 4 && $1 == "S" { print }' \
    | sort -t$'\t' -k2,2 -k3,3 -k4,4
}

oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_log dl_status
  dl_log=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'L' || char(9) || commit_hash || char(9) || message || char(9) || coalesce(email, '') || char(9) || coalesce(date, '') FROM dolt_log;\n" "$setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize_log)
  dl_status=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'S' || char(9) || table_name || char(9) || staged || char(9) || status FROM dolt_status;\n" "$setup" \
              | "$DOLTLITE" "$dir/dl/db.s" 2>>"$dir/dl.err" \
              | grep -v '^[0-9]*$' \
              | grep -v '^[0-9a-f]\{40\}$' \
              | normalize_status)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_log dt_status
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat('L', char(9), commit_hash, char(9), message, char(9), coalesce(email, ''), char(9), coalesce(cast(date as char), '')) FROM dolt_log ORDER BY commit_order DESC;" 2>>"$dir/dt.err"
  ) > "$dir/dt.log.raw"
  dt_log=$(tail -n +2 "$dir/dt.log.raw" | tr -d '"' | normalize_log)

  (
    mkdir -p "$dir/dt.s" && cd "$dir/dt.s" || exit 1
    vc_oracle_init_repo
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.s.err"
    "$DOLT" sql -r csv -q "SELECT concat('S', char(9), table_name, char(9), staged, char(9), status) FROM dolt_status;" 2>>"$dir/dt.s.err"
  ) > "$dir/dt.status.raw"
  dt_status=$(tail -n +2 "$dir/dt.status.raw" | tr -d '"' | normalize_status)

  if [ -z "$dl_log" ] && [ -z "$dt_log" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (empty log on both sides — query likely errored)"
    echo "    doltlite stderr:"; tail -3 "$dir/dl.err" | sed 's/^/      /'
    echo "    dolt stderr:";     tail -3 "$dir/dt.err" | sed 's/^/      /'
    return
  fi

  local dl_combined dt_combined
  dl_combined="$dl_log"$'\n'"$dl_status"
  dt_combined="$dt_log"$'\n'"$dt_status"

  vc_oracle_assert_match "$name" "$dl_combined" "$dt_combined"
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

  if [ "$dl_rc" -ne 0 ] && [ "$dt_rc" -ne 0 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
  fi
}

echo "=== Version Control Oracle Tests: dolt_commit ==="
echo ""

echo "--- message argument forms ---"

oracle "commit_short_m_flag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first commit');
"

oracle "commit_long_message_flag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('--message', 'first commit');
"

echo "--- combo / stage-all flags ---"

oracle "commit_uppercase_A_new_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-A', '-m', 'first commit');
"

oracle_error "commit_all_long_new_table_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('--all', '-m', 'first commit');
"

oracle "commit_all_long_modified_tracked_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
INSERT INTO t VALUES (2, 20);
SELECT dolt_commit('--all', '-m', 'modify');
"

oracle_error "commit_lowercase_a_new_table_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-a', '-m', 'first commit');
"

oracle "commit_lowercase_a_modified_tracked_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
INSERT INTO t VALUES (2, 20);
SELECT dolt_commit('-a', '-m', 'modify');
"

oracle "commit_combo_am_modified_tracked_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
INSERT INTO t VALUES (2, 20);
SELECT dolt_commit('-am', 'modify');
"

oracle_error "commit_combo_am_new_table_errors" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-am', 'first commit');
"

echo "--- author override ---"

oracle "commit_author_name_and_email" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first', '--author', 'Alice Author <alice@example.com>');
"

oracle "commit_author_name_only" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first', '--author', 'Bob Bare-Name <bob@example.com>');
"

echo "--- --amend ---"

oracle "commit_amend_message_only" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'original');
SELECT dolt_commit('--amend', '-m', 'amended');
"

oracle "commit_amend_with_new_content" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'original');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('--amend', '-m', 'amended with row 2');
"

echo "--- skip / allow empty ---"

oracle "commit_allow_empty_no_changes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_commit('--allow-empty', '-m', 'empty followup');
"

oracle "commit_skip_empty_no_changes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_commit('--skip-empty', '-m', 'second');
"

oracle "commit_skip_empty_with_changes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('--skip-empty', '-m', 'second');
"

echo "--- --date ---"

oracle "commit_with_explicit_date" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first', '--date', '2024-01-15T10:00:00Z');
"

echo "--- schema edge commits ---"

oracle "commit_renamed_and_modified_table" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
ALTER TABLE a RENAME TO b;
INSERT INTO b VALUES (2, 'x');
SELECT dolt_add('b');
SELECT dolt_commit('-m', 'rename and edit');
"

oracle "commit_recreated_same_name_table" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
SELECT dolt_add('a');
SELECT dolt_commit('-m', 'recreate a');
"

oracle "commit_schema_staged_data_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
ALTER TABLE t ADD COLUMN n INTEGER;
SELECT dolt_add('t');
INSERT INTO t VALUES (2, 'x', 2);
SELECT dolt_commit('-m', 'schema only staged');
"

oracle "commit_drop_one_modify_one" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
DROP TABLE a;
INSERT INTO b VALUES (2, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop and edit');
"

echo "--- error paths ---"

oracle_error "commit_no_message" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit();
"

oracle_error "commit_empty_message" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', '');
"

oracle_error "commit_extra_positional_arg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first', 'extra');
"

oracle_error "commit_missing_short_message_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m');
"

oracle_error "commit_missing_long_message_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('--message');
"

oracle_error "commit_missing_author_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first', '--author');
"

oracle_error "commit_missing_date_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first', '--date');
"

oracle_error "commit_unknown_short_flag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-z', '-m', 'first');
"

oracle_error "commit_unknown_long_flag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('--bogus', '-m', 'first');
"

oracle_error "commit_nothing_staged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_commit('-m', 'nothing-to-do');
"

oracle_error "commit_with_unresolved_conflicts" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
SELECT dolt_commit('-m', 'force-commit-with-conflict');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
