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

oracle_query() {
  local name="$1" setup="$2" dl_query="$3" dt_query="$4"
  local dir="$TMPROOT/${name}_query"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out dt_out dolt_setup dolt_query
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$dl_query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' | grep '^R|' | sort)

  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  dolt_query=$(vc_oracle_translate_for_dolt "$dt_query")
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf '%s\n' "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.setup.err"
    printf '%s\n' "$dolt_query" | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tail -n +2 "$dir/dt.raw" | tr -d '"\r' | grep '^R|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_query_dual() {
  local name="$1" dl_setup="$2" dt_setup="$3" dl_query="$4" dt_query="$5"
  local dir="$TMPROOT/${name}_dual"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out dt_out dolt_setup dolt_query
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$dl_setup" "$dl_query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' | grep '^R|' | sort)

  dolt_setup=$(vc_oracle_translate_for_dolt "$dt_setup")
  dolt_query=$(vc_oracle_translate_for_dolt "$dt_query")
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf '%s\n' "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.setup.err"
    printf '%s\n' "$dolt_query" | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tail -n +2 "$dir/dt.raw" | tr -d '"\r' | grep '^R|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
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

oracle "commit_author_missing_closing_bracket" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first', '--author', 'Alice Author <alice@example.com');
"

oracle_error "commit_malformed_author" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first', '--author', 'not-an-author');
"

oracle_error "commit_empty_author_email" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first', '--author', 'Alice Author <>');
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

# Amending a merge must keep every parent. Dropping all but the first takes the
# merged branch out of ancestry, so the log loses a commit and a later merge of
# the same branch is no longer a no-op.
oracle "commit_amend_merge_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_checkout('-b', 'side');
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'side1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main1');
SELECT dolt_merge('side');
SELECT dolt_commit('--amend', '-m', 'amended merge');
"

oracle_query "commit_amend_merge_keeps_parents" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_checkout('-b', 'side');
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'side1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main1');
SELECT dolt_merge('side');
SELECT dolt_commit('--amend', '-m', 'amended merge');
" \
"SELECT 'R|parents|' || count(*) FROM dolt_commit_ancestors
   WHERE commit_hash=(SELECT dolt_hashof('HEAD'))
 UNION ALL SELECT 'R|log|' || count(*) FROM dolt_log;" \
"SELECT CONCAT('R|parents|', count(*)) FROM dolt_commit_ancestors
   WHERE commit_hash=hashof('HEAD')
 UNION ALL SELECT CONCAT('R|log|', count(*)) FROM dolt_log;"

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

echo "--- commit -am catalog adoption edges ---"

oracle_query "commit_am_schema_only_add_column" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
ALTER TABLE t ADD COLUMN note TEXT DEFAULT 'n/a';
SELECT dolt_commit('-am', 'add note');
" \
"SELECT 'R|col|' || name FROM pragma_table_info('t')
 UNION ALL SELECT 'R|status|' || count(*) FROM dolt_status
 UNION ALL SELECT 'R|log|' || message FROM dolt_log;" \
"SELECT CONCAT('R|col|', column_name) FROM information_schema.columns WHERE table_name='t'
 UNION ALL SELECT CONCAT('R|status|', count(*)) FROM dolt_status
 UNION ALL SELECT CONCAT('R|log|', message) FROM dolt_log;"

oracle_query "commit_am_data_and_schema" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
ALTER TABLE t ADD COLUMN note TEXT DEFAULT 'n/a';
INSERT INTO t(id, v, note) VALUES (2, 'second', 'explicit');
UPDATE t SET note='updated' WHERE id=1;
SELECT dolt_commit('-am', 'schema and data');
" \
"SELECT 'R|row|' || id || '|' || v || '|' || note FROM t
 UNION ALL SELECT 'R|col|' || name FROM pragma_table_info('t')
 UNION ALL SELECT 'R|status|' || count(*) FROM dolt_status;" \
"SELECT CONCAT('R|row|', id, '|', v, '|', note) FROM t
 UNION ALL SELECT CONCAT('R|col|', column_name) FROM information_schema.columns WHERE table_name='t'
 UNION ALL SELECT CONCAT('R|status|', count(*)) FROM dolt_status;"

oracle_query "commit_am_drop_recreate_same_name" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'old');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
DROP TABLE t;
CREATE TABLE t(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES (7, 70);
SELECT dolt_commit('-am', 'recreate t');
" \
"SELECT 'R|row|' || k || '|' || n FROM t
 UNION ALL SELECT 'R|col|' || name FROM pragma_table_info('t')
 UNION ALL SELECT 'R|status|' || count(*) FROM dolt_status;" \
"SELECT CONCAT('R|row|', k, '|', n) FROM t
 UNION ALL SELECT CONCAT('R|col|', column_name) FROM information_schema.columns WHERE table_name='t'
 UNION ALL SELECT CONCAT('R|status|', count(*)) FROM dolt_status;"

oracle_query "commit_am_index_and_view" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE INDEX t_v_idx ON t(v);
CREATE VIEW high_t AS SELECT id, v FROM t WHERE v >= 20;
INSERT INTO t VALUES (1, 10), (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
INSERT INTO t VALUES (3, 30);
UPDATE t SET v=25 WHERE id=2;
SELECT dolt_commit('-am', 'data with index view');
" \
"SELECT 'R|high|' || id || '|' || v FROM high_t
 UNION ALL SELECT 'R|index|' || name FROM pragma_index_list('t') WHERE name='t_v_idx'
 UNION ALL SELECT 'R|schema|' || type || '|' || name FROM sqlite_master WHERE name='high_t'
 UNION ALL SELECT 'R|status|' || count(*) FROM dolt_status;" \
"SELECT CONCAT('R|high|', id, '|', v) FROM high_t
 UNION ALL SELECT CONCAT('R|index|', index_name) FROM information_schema.statistics WHERE table_name='t' AND index_name='t_v_idx'
 UNION ALL SELECT CONCAT('R|schema|', type, '|', name) FROM dolt_schemas WHERE name='high_t'
 UNION ALL SELECT CONCAT('R|status|', count(*)) FROM dolt_status;"

oracle_query_dual "commit_am_trigger_persists" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE log(id INTEGER, v INTEGER);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
INSERT INTO t VALUES (2, 20);
SELECT dolt_commit('-am', 'trigger and data');
INSERT INTO t VALUES (3, 30);
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE log(id INTEGER, v INTEGER);
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
INSERT INTO t VALUES (2, 20);
SELECT dolt_commit('-am', 'trigger and data');
INSERT INTO t VALUES (3, 30);
" \
"SELECT 'R|log|' || id || '|' || v FROM log;" \
"SELECT CONCAT('R|log|', id, '|', v) FROM log;"

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

echo "--- commit -a leaves a working-tree rename in the working tree ---"

COMMIT_A_RENAME_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX tvi ON t(v);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
ALTER TABLE t RENAME TO t2;
"

oracle "commit_a_leaves_unstaged_rename_alone" "
$COMMIT_A_RENAME_SEED
SELECT dolt_commit('-am', 'nothing stageable');
"

oracle_query "commit_a_unstaged_rename_head_keeps_table" "
$COMMIT_A_RENAME_SEED
SELECT dolt_commit('-am', 'nothing stageable');
SELECT dolt_reset('--hard');
" "SELECT 'R|' || id || '|' || v FROM t;" "SELECT concat('R|', id, '|', v) FROM t;"

COMMIT_A_SHIFT_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE u(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
INSERT INTO u VALUES (2, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
ALTER TABLE t RENAME TO z;
INSERT INTO u VALUES (3, 3);
"

oracle "commit_a_order_shifting_rename_stays_unstaged" "
$COMMIT_A_SHIFT_SEED
SELECT dolt_commit('-am', 'rename and edit');
"

oracle_query "commit_a_order_shifting_rename_head_keeps_table" "
$COMMIT_A_SHIFT_SEED
SELECT dolt_commit('-am', 'rename and edit');
SELECT dolt_reset('--hard');
" "SELECT 'R|t|' || id || '|' || v FROM t;
SELECT 'R|u|' || id || '|' || v FROM u;" "SELECT concat('R|t|', id, '|', v) FROM t;
SELECT concat('R|u|', id, '|', v) FROM u;"

oracle "commit_a_drop_create_commits_drop_only" "
CREATE TABLE m(a INT PRIMARY KEY, b INT);
CREATE TABLE n(a INT PRIMARY KEY);
INSERT INTO m VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
DROP TABLE m;
CREATE TABLE a(x INT PRIMARY KEY, y INT);
INSERT INTO a VALUES (9, 9);
SELECT dolt_commit('-am', 'drop only');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
