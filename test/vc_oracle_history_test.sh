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

normalize_history() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 6 && $1 == "H" { print }' \
    | awk -F'\t' '
        {
          tbl = $2
          id  = $3
          v   = $4
          msg = $5
          who = $6
          if (msg ~ /^Revert "/) {
            sub(/^Revert "/, "Revert ", msg)
            sub(/"$/, "", msg)
          }
          if (who == "" \
           || who == "root" \
           || who == "oracle" \
           || who == "doltlite") {
            who = "DEFAULT"
          }
          print "H\t" tbl "\t" id "\t" v "\t" msg "\t" who
        }
      ' \
    | sort
}

oracle() {
  local name="$1" setup="$2" tables="${3:-t}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  IFS=',' read -ra tarr <<< "$tables"

  local dl_q=""
  for tn in "${tarr[@]}"; do
    local part="SELECT 'H' || char(9) || '${tn}' || char(9) || coalesce(h.id,'') || char(9) || coalesce(h.v,'') || char(9) || coalesce(log.message, h.commit_hash) || char(9) || coalesce(h.committer,'') FROM dolt_history_${tn} h LEFT JOIN dolt_log log ON log.commit_hash = h.commit_hash"
    if [ -z "$dl_q" ]; then
      dl_q="$part"
    else
      dl_q="$dl_q UNION ALL $part"
    fi
  done

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$dl_q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize_history)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_q=""
  for tn in "${tarr[@]}"; do
    local part="SELECT concat('H', char(9), '${tn}', char(9), coalesce(h.id,''), char(9), coalesce(h.v,''), char(9), coalesce(log.message, h.commit_hash), char(9), coalesce(h.committer,'')) FROM dolt_history_${tn} h LEFT JOIN dolt_log log ON log.commit_hash = h.commit_hash"
    if [ -z "$dt_q" ]; then
      dt_q="$part"
    else
      dt_q="$dt_q UNION ALL $part"
    fi
  done

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$dt_q"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" | tr -d '"' | normalize_history
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

noncurrent_oracle() {
  local name="$1" setup="$2" dl_query="$3" dt_query="$4"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  printf "%s\n" "$setup" | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.setup" 2>"$dir/dl.setup.err"
  local dl_out
  dl_out=$(printf ".headers off\n.mode list\n%s\n" "$dl_query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' | grep '^H|' | sort)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_out
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf '%s\n' "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.setup.err"
    printf '%s\n' "$dt_query" | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^H|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: dolt_history_<table> ==="
echo ""

SEED="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- basic ---"

oracle "single_commit_two_rows" "
$SEED
"

oracle "modify_one_row_then_query" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_mod');
"

oracle "insert_new_row" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add');
"

oracle "delete_row_in_later_commit" "
$SEED
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_del');
"

oracle "many_commits_many_changes" "
$SEED
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3');
UPDATE t SET v = 200 WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4');
DELETE FROM t WHERE id = 3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c5');
"

echo "--- working set is excluded ---"

oracle "working_set_changes_not_in_history" "
$SEED
UPDATE t SET v = 999 WHERE id = 1;
"

oracle "staged_changes_not_in_history" "
$SEED
UPDATE t SET v = 999 WHERE id = 1;
SELECT dolt_add('-A');
"

echo "--- multi-table ---"

oracle "two_tables_independent_history" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO u VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init_both');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_t');
INSERT INTO u VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
" "t,u"

echo "--- branching ---"

NONCURRENT_SETUP="
SELECT dolt_checkout('-b', 'feature');
CREATE TABLE feature_only(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO feature_only VALUES(1, 'feature');
SELECT dolt_commit('-A', '-m', 'feature-only table');
INSERT INTO feature_only VALUES(2, 'feature-2');
SELECT dolt_commit('-A', '-m', 'feature-only second commit');
SELECT dolt_checkout('main');
"

noncurrent_oracle "history_exact_commit_on_sibling_branch_after_reopen" \
  "$NONCURRENT_SETUP" \
  "SELECT CONCAT('H|', id, '|', v) FROM dolt_history_feature_only WHERE commit_hash = dolt_hashof('feature');" \
  "SELECT CONCAT('H|', id, '|', v) FROM feature_only AS OF 'feature';"

noncurrent_oracle "history_start_ref_on_sibling_branch_after_reopen" \
  "$NONCURRENT_SETUP" \
  "SELECT CONCAT('H|', id, '|', v) FROM dolt_history_feature_only('feature');" \
  "SELECT CONCAT('H|', id, '|', v) FROM dolt_history_feature_only AS OF 'feature';"

JOIN_ORDER_SETUP="
CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO items VALUES(1, 'main');
SELECT dolt_commit('-A', '-m', 'main');
SELECT dolt_checkout('-b', 'feature');
INSERT INTO items VALUES(2, 'feature-transient');
SELECT dolt_commit('-A', '-m', 'feature first');
DELETE FROM items WHERE id = 2;
INSERT INTO items VALUES(3, 'feature-head');
SELECT dolt_commit('-A', '-m', 'feature head');
SELECT dolt_checkout('main');
"

JOIN_ANCESTRY="WITH RECURSIVE ancestry(commit_hash) AS (
  SELECT dolt_hashof('feature')
  UNION ALL
  SELECT parents.parent_hash
  FROM ancestry
  JOIN dolt_commit_ancestors parents
    ON parents.commit_hash = ancestry.commit_hash
   AND parents.parent_index = 0
  WHERE parents.parent_hash IS NOT NULL
)"

noncurrent_oracle "history_noncurrent_ordinary_join_uses_session_head" \
  "$JOIN_ORDER_SETUP" \
  "$JOIN_ANCESTRY SELECT CONCAT('H|', count(*)) FROM ancestry JOIN dolt_history_items history ON history.commit_hash = ancestry.commit_hash;" \
  "$JOIN_ANCESTRY SELECT CONCAT('H|', count(*)) FROM ancestry JOIN dolt_history_items history ON history.commit_hash = ancestry.commit_hash;"

noncurrent_oracle "history_noncurrent_cross_join_uses_session_head" \
  "$JOIN_ORDER_SETUP" \
  "$JOIN_ANCESTRY SELECT CONCAT('H|', count(*)) FROM ancestry CROSS JOIN dolt_history_items history WHERE history.commit_hash = ancestry.commit_hash;" \
  "$JOIN_ANCESTRY SELECT CONCAT('H|', count(*)) FROM ancestry CROSS JOIN dolt_history_items history WHERE history.commit_hash = ancestry.commit_hash;"

oracle "history_on_feature_branch" "
$SEED
SELECT dolt_branch('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
"

oracle "history_on_branch_created_from_tag" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_tag('v1', 'HEAD~1');
SELECT dolt_branch('from_tag', 'v1');
SELECT dolt_checkout('from_tag');
"

oracle "history_after_merge" "
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
"

oracle "history_on_branch_created_from_first_parent" "
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
SELECT dolt_branch('from_p1', 'HEAD^1');
SELECT dolt_checkout('from_p1');
"

oracle "history_on_branch_created_from_second_parent" "
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
SELECT dolt_branch('from_p2', 'HEAD^2');
SELECT dolt_checkout('from_p2');
"

oracle "history_replay_merge_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES (1, 'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
" "u"

oracle "history_replay_cherrypick_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES (1, 'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feat');
" "u"

oracle "history_replay_revert_schema_change_with_later_added_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES (1, 'later');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message='main_check' LIMIT 1));
" "u"

oracle "history_replay_rebase_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES (1, 'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
" "u"

oracle "history_replay_merge_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, v INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, v INT, FOREIGN KEY (v) REFERENCES p(v));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
" "c"

oracle "history_replay_cherrypick_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, v INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, v INT, FOREIGN KEY (v) REFERENCES p(v));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feat');
" "c"

oracle "history_replay_rebase_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, v INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, v INT, FOREIGN KEY (v) REFERENCES p(v));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
" "c"

echo "--- edge cases ---"

oracle "single_row_single_commit" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'just_one');
"

oracle "same_row_value_churned" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE t SET v = 2 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
UPDATE t SET v = 3 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3');
UPDATE t SET v = 4 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4');
"

oracle "add_delete_readd_same_id" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1_add');
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_del');
INSERT INTO t VALUES (1, 999);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_readd');
"

oracle "history_after_checkout_sibling_branch" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_only');
SELECT dolt_checkout('main');
"

oracle "history_after_hard_reset" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3');
SELECT dolt_reset('--hard', 'HEAD~1');
"

oracle "history_after_amend" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_original');
SELECT dolt_commit('--amend', '-m', 'c2_amended');
"

echo "--- NULL values and other types ---"

oracle "null_value_in_history" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE t SET v = NULL WHERE id = 1;
UPDATE t SET v = 99 WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_swap_nulls');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
