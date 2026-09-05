#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

run_pair() {
  local name="$1" setup="$2" dl_query="$3" dt_query="$4" in_merge="${5:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_setup="$setup" dt_prefix=""
  if [ "$in_merge" = "IN_MERGE" ]; then
    dl_setup=$(printf '%s\n' "$setup" | perl -0pe "s/\nSELECT dolt_merge\\(/\nBEGIN;\\nSELECT dolt_merge\\(/")
    dt_prefix="SET @@autocommit = 0;"
  fi

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$dl_setup" "$dl_query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^R|' | sort)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      [ -n "$dt_prefix" ] && printf '%s\n' "$dt_prefix"
      printf '%s\n' "$dolt_setup"
      printf '%s\n' "$dt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle() {
  run_pair "$1" "$2" "$3" "$3"
}

oracle_in_merge() {
  run_pair "$1" "$2" "$3" "$3" IN_MERGE
}

# DoltLite spells `t AS OF 'ref'` as dolt_at_t filtered on commit_ref.
oracle_as_of() {
  run_pair "$1" "$2" "$3" "$4"
}

echo "=== Version Control Oracle Tests: joins over version-control read tables ==="
echo ""

LINEAR="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, w TEXT);
INSERT INTO t VALUES (1, 10), (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE t SET v = 11 WHERE id = 1;
INSERT INTO u VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
INSERT INTO t VALUES (3, 30);
DELETE FROM t WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3');
UPDATE u SET w = 'b' WHERE id = 1;
INSERT INTO u VALUES (2, 'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4');
"

BRANCHY="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_tag('v1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat2');
SELECT dolt_checkout('main');
UPDATE t SET v = 200 WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main1');
SELECT dolt_branch('side');
SELECT dolt_merge('feat', '-m', 'merge_feat');
SELECT dolt_tag('v2');
"

echo "--- dolt_log x dolt_commit_ancestors ---"

oracle "log_parent_messages_linear" "$LINEAR" \
"SELECT CONCAT('R|', c.message, '|', coalesce(p.message, 'NONE'), '|', a.parent_index)
 FROM dolt_log c
 JOIN dolt_commit_ancestors a ON a.commit_hash = c.commit_hash
 LEFT JOIN dolt_log p ON p.commit_hash = a.parent_hash;"

oracle "log_parent_messages_merge" "$BRANCHY" \
"SELECT CONCAT('R|', c.message, '|', coalesce(p.message, 'NONE'), '|', a.parent_index)
 FROM dolt_log c
 JOIN dolt_commit_ancestors a ON a.commit_hash = c.commit_hash
 LEFT JOIN dolt_log p ON p.commit_hash = a.parent_hash;"

oracle "log_parent_count_per_commit" "$BRANCHY" \
"SELECT CONCAT('R|', c.message, '|', count(a.parent_hash))
 FROM dolt_log c
 LEFT JOIN dolt_commit_ancestors a ON a.commit_hash = c.commit_hash
 GROUP BY c.message;"

oracle "log_grandparent_two_hops" "$LINEAR" \
"SELECT CONCAT('R|', c.message, '|', g.message)
 FROM dolt_log c
 JOIN dolt_commit_ancestors a1 ON a1.commit_hash = c.commit_hash
 JOIN dolt_commit_ancestors a2 ON a2.commit_hash = a1.parent_hash
 JOIN dolt_log g ON g.commit_hash = a2.parent_hash;"

oracle "log_childless_tips_antijoin" "$BRANCHY" \
"SELECT CONCAT('R|', c.message)
 FROM dolt_log c
 LEFT JOIN dolt_commit_ancestors a ON a.parent_hash = c.commit_hash
 WHERE a.commit_hash IS NULL;"

oracle "log_merge_parents_side_by_side" "$BRANCHY" \
"SELECT CONCAT('R|', c.message, '|', p0.message, '|', p1.message)
 FROM dolt_log c
 JOIN dolt_commit_ancestors a0 ON a0.commit_hash = c.commit_hash AND a0.parent_index = 0
 JOIN dolt_commit_ancestors a1 ON a1.commit_hash = c.commit_hash AND a1.parent_index = 1
 JOIN dolt_log p0 ON p0.commit_hash = a0.parent_hash
 JOIN dolt_log p1 ON p1.commit_hash = a1.parent_hash;"

oracle "log_ancestors_cte_tip_parents" "$BRANCHY" \
"WITH tip AS (SELECT hash FROM dolt_branches WHERE name = 'main')
 SELECT CONCAT('R|', p.message)
 FROM tip
 JOIN dolt_commit_ancestors a ON a.commit_hash = tip.hash
 JOIN dolt_log p ON p.commit_hash = a.parent_hash;"

echo "--- dolt_log x dolt_diff ---"

oracle "log_left_join_diff_tables" "$LINEAR" \
"SELECT CONCAT('R|', l.message, '|', coalesce(d.table_name, 'NONE'), '|', coalesce(d.data_change, 'NONE'), '|', coalesce(d.schema_change, 'NONE'))
 FROM dolt_log l
 LEFT JOIN dolt_diff d ON d.commit_hash = l.commit_hash;"

oracle "diff_right_join_log" "$LINEAR" \
"SELECT CONCAT('R|', l.message, '|', coalesce(d.table_name, 'NONE'))
 FROM dolt_diff d
 RIGHT JOIN dolt_log l ON d.commit_hash = l.commit_hash;"

oracle "ancestors_right_join_log" "$BRANCHY" \
"SELECT CONCAT('R|', l.message, '|', coalesce(p.message, 'ROOT'))
 FROM dolt_log p
 RIGHT JOIN dolt_commit_ancestors a ON a.parent_hash = p.commit_hash
 JOIN dolt_log l ON l.commit_hash = a.commit_hash;"

oracle "diff_self_join_tables_changed_together" "$LINEAR" \
"SELECT CONCAT('R|', l.message, '|', d1.table_name, '|', d2.table_name)
 FROM dolt_diff d1
 JOIN dolt_diff d2 ON d1.commit_hash = d2.commit_hash AND d1.table_name < d2.table_name
 JOIN dolt_log l ON l.commit_hash = d1.commit_hash;"

oracle "diff_tables_per_commit_having" "$LINEAR" \
"SELECT CONCAT('R|', l.message, '|', count(*))
 FROM dolt_log l
 JOIN dolt_diff d ON d.commit_hash = l.commit_hash
 GROUP BY l.message
 HAVING count(*) > 1;"

oracle "log_in_subquery_diff" "$LINEAR" \
"SELECT CONCAT('R|', message) FROM dolt_log
 WHERE commit_hash IN (SELECT commit_hash FROM dolt_diff WHERE table_name = 'u');"

oracle "log_exists_diff" "$LINEAR" \
"SELECT CONCAT('R|', l.message) FROM dolt_log l
 WHERE EXISTS (SELECT 1 FROM dolt_diff d WHERE d.commit_hash = l.commit_hash AND d.table_name = 't');"

oracle "log_not_exists_diff" "$LINEAR" \
"SELECT CONCAT('R|', l.message) FROM dolt_log l
 WHERE NOT EXISTS (SELECT 1 FROM dolt_diff d WHERE d.commit_hash = l.commit_hash);"

oracle "diff_using_join_commits_per_table" "$LINEAR" \
"SELECT CONCAT('R|', d.table_name, '|', count(*), '|', min(l.message), '|', max(l.message))
 FROM dolt_diff d JOIN dolt_log l USING (commit_hash)
 GROUP BY d.table_name;"

oracle "diff_join_log_on_merge_history" "$BRANCHY" \
"SELECT CONCAT('R|', l.message, '|', d.table_name, '|', d.data_change, '|', d.schema_change)
 FROM dolt_diff d JOIN dolt_log l ON l.commit_hash = d.commit_hash;"

echo "--- dolt_history_<table> joins ---"

oracle "history_join_log_filtered_by_id_and_message" "$LINEAR" \
"SELECT CONCAT('R|', h.id, '|', coalesce(h.v, 'NULL'), '|', l.message)
 FROM dolt_history_t h JOIN dolt_log l ON l.commit_hash = h.commit_hash
 WHERE h.id = 1 AND l.message <> 'c3';"

oracle "history_consecutive_value_changes" "$LINEAR" \
"SELECT CONCAT('R|', n.id, '|', coalesce(o.v, 'NULL'), '|', coalesce(n.v, 'NULL'), '|', ln.message, '|', lo.message)
 FROM dolt_history_t n
 JOIN dolt_commit_ancestors a ON a.commit_hash = n.commit_hash
 JOIN dolt_history_t o ON o.commit_hash = a.parent_hash AND o.id = n.id
 JOIN dolt_log ln ON ln.commit_hash = n.commit_hash
 JOIN dolt_log lo ON lo.commit_hash = a.parent_hash
 WHERE o.v <> n.v;"

oracle "history_rows_added_vs_parent" "$LINEAR" \
"SELECT CONCAT('R|', n.id, '|', ln.message)
 FROM dolt_history_t n
 JOIN dolt_commit_ancestors a ON a.commit_hash = n.commit_hash
 JOIN dolt_log ln ON ln.commit_hash = n.commit_hash
 LEFT JOIN dolt_history_t o ON o.commit_hash = a.parent_hash AND o.id = n.id
 WHERE o.id IS NULL;"

oracle "history_two_tables_same_commit" "$LINEAR" \
"SELECT CONCAT('R|', l.message, '|', ht.id, '|', ht.v, '|', hu.id, '|', hu.w)
 FROM dolt_history_t ht
 JOIN dolt_history_u hu ON hu.commit_hash = ht.commit_hash
 JOIN dolt_log l ON l.commit_hash = ht.commit_hash;"

oracle "history_vs_current_values" "$LINEAR" \
"SELECT CONCAT('R|', h.id, '|', h.v, '|', t.v, '|', l.message)
 FROM dolt_history_t h
 JOIN t ON t.id = h.id
 JOIN dolt_log l ON l.commit_hash = h.commit_hash
 WHERE h.v <> t.v;"

oracle "history_ids_no_longer_present" "$LINEAR" \
"SELECT DISTINCT CONCAT('R|', h.id)
 FROM dolt_history_t h LEFT JOIN t ON t.id = h.id
 WHERE t.id IS NULL;"

oracle "history_at_branch_tips" "$BRANCHY" \
"SELECT CONCAT('R|', b.name, '|', h.id, '|', h.v)
 FROM dolt_branches b JOIN dolt_history_t h ON h.commit_hash = b.hash;"

oracle "history_at_tags" "$BRANCHY" \
"SELECT CONCAT('R|', g.tag_name, '|', h.id, '|', h.v)
 FROM dolt_tags g JOIN dolt_history_t h ON h.commit_hash = g.tag_hash;"

oracle "history_row_count_per_commit" "$BRANCHY" \
"SELECT CONCAT('R|', l.message, '|', count(*), '|', sum(h.v))
 FROM dolt_history_t h JOIN dolt_log l ON l.commit_hash = h.commit_hash
 GROUP BY l.message;"

oracle "history_cross_join_log_outer" "$LINEAR" \
"SELECT CONCAT('R|', l.message, '|', h.id, '|', coalesce(h.v, 'NULL'))
 FROM dolt_log l CROSS JOIN dolt_history_t h
 WHERE h.commit_hash = l.commit_hash AND l.message IN ('c1', 'c3');"

oracle "history_cross_join_log_inner" "$LINEAR" \
"SELECT CONCAT('R|', l.message, '|', h.id, '|', coalesce(h.v, 'NULL'))
 FROM dolt_history_t h CROSS JOIN dolt_log l
 WHERE h.commit_hash = l.commit_hash AND h.id = 3;"

oracle "history_join_hash_from_branch_subquery" "$BRANCHY" \
"SELECT CONCAT('R|', h.id, '|', h.v)
 FROM dolt_history_t h
 WHERE h.commit_hash = (SELECT hash FROM dolt_branches WHERE name = 'side');"

oracle "history_join_hash_from_tag_subquery" "$BRANCHY" \
"SELECT CONCAT('R|', h.id, '|', h.v)
 FROM dolt_history_t h
 WHERE h.commit_hash IN (SELECT tag_hash FROM dolt_tags WHERE tag_name = 'v1');"

echo "--- dolt_diff_<table> joins ---"

oracle "difft_join_log_to_commit" "$LINEAR" \
"SELECT CONCAT('R|', l.message, '|', d.diff_type, '|', coalesce(d.from_id, 'NULL'), '|', coalesce(d.from_v, 'NULL'), '|', coalesce(d.to_id, 'NULL'), '|', coalesce(d.to_v, 'NULL'))
 FROM dolt_diff_t d JOIN dolt_log l ON l.commit_hash = d.to_commit;"

oracle "difft_join_log_from_commit" "$LINEAR" \
"SELECT CONCAT('R|', l.message, '|', d.diff_type, '|', coalesce(d.to_id, 'NULL'))
 FROM dolt_diff_t d JOIN dolt_log l ON l.commit_hash = d.from_commit;"

oracle "difft_both_endpoints_named" "$BRANCHY" \
"SELECT CONCAT('R|', lf.message, '|', lt.message, '|', d.diff_type, '|', coalesce(d.to_id, d.from_id))
 FROM dolt_diff_t d
 JOIN dolt_log lf ON lf.commit_hash = d.from_commit
 JOIN dolt_log lt ON lt.commit_hash = d.to_commit;"

oracle "log_right_join_difft_working_unmatched" "
$LINEAR
UPDATE t SET v = 999 WHERE id = 1;
" \
"SELECT CONCAT('R|', coalesce(l.message, 'NO_COMMIT'), '|', d.diff_type, '|', coalesce(d.to_id, d.from_id), '|', coalesce(d.to_v, 'NULL'))
 FROM dolt_log l
 RIGHT JOIN dolt_diff_t d ON d.to_commit = l.commit_hash;"

oracle "log_right_join_history" "$LINEAR" \
"SELECT CONCAT('R|', coalesce(l.message, 'NONE'), '|', h.id, '|', coalesce(h.v, 'NULL'))
 FROM dolt_log l
 RIGHT JOIN dolt_history_t h ON h.commit_hash = l.commit_hash AND l.message <> 'c2';"

oracle "difft_two_tables_same_commit" "$LINEAR" \
"SELECT CONCAT('R|', l.message, '|', dt.diff_type, '|', coalesce(dt.to_id, dt.from_id), '|', du.diff_type, '|', coalesce(du.to_id, du.from_id))
 FROM dolt_diff_t dt
 JOIN dolt_diff_u du ON du.to_commit = dt.to_commit
 JOIN dolt_log l ON l.commit_hash = dt.to_commit;"

oracle "difft_matches_history_values" "$LINEAR" \
"SELECT CONCAT('R|', d.to_id, '|', d.diff_type, '|', CASE WHEN h.v = d.to_v THEN 'same' ELSE 'DIFFERENT' END)
 FROM dolt_diff_t d
 JOIN dolt_history_t h ON h.commit_hash = d.to_commit AND h.id = d.to_id
 WHERE d.diff_type <> 'removed';"

oracle "difft_working_rows_left_join_log" "
$LINEAR
UPDATE t SET v = 999 WHERE id = 1;
INSERT INTO t VALUES (9, 90);
" \
"SELECT CONCAT('R|', coalesce(l.message, d.to_commit), '|', d.diff_type, '|', coalesce(d.to_id, d.from_id), '|', coalesce(d.from_v, 'NULL'), '|', coalesce(d.to_v, 'NULL'))
 FROM dolt_diff_t d LEFT JOIN dolt_log l ON l.commit_hash = d.to_commit;"

oracle "difft_row_count_per_dolt_diff_row" "$LINEAR" \
"SELECT CONCAT('R|', l.message, '|', c.table_name, '|', count(d.diff_type))
 FROM dolt_diff c
 JOIN dolt_log l ON l.commit_hash = c.commit_hash
 LEFT JOIN dolt_diff_t d ON d.to_commit = c.commit_hash AND c.table_name = 't'
 GROUP BY l.message, c.table_name;"

oracle "difft_join_current_table_state" "$LINEAR" \
"SELECT CONCAT('R|', l.message, '|', d.to_id, '|', d.to_v, '|', coalesce(t.v, 'GONE'))
 FROM dolt_diff_t d
 JOIN dolt_log l ON l.commit_hash = d.to_commit
 LEFT JOIN t ON t.id = d.to_id
 WHERE d.diff_type <> 'removed';"

echo "--- branches, tags, refs ---"

oracle "branches_join_log_tip_messages" "$BRANCHY" \
"SELECT CONCAT('R|', b.name, '|', l.message)
 FROM dolt_branches b JOIN dolt_log l ON l.commit_hash = b.hash;"

oracle "branches_left_join_log_tip_messages" "
$BRANCHY
SELECT dolt_checkout('side');
" \
"SELECT CONCAT('R|', b.name, '|', coalesce(l.message, 'NOT_IN_LOG'))
 FROM dolt_branches b LEFT JOIN dolt_log l ON l.commit_hash = b.hash;"

oracle "tags_join_log_messages" "$BRANCHY" \
"SELECT CONCAT('R|', g.tag_name, '|', l.message)
 FROM dolt_tags g JOIN dolt_log l ON l.commit_hash = g.tag_hash;"

oracle "tags_join_branches_same_commit" "$BRANCHY" \
"SELECT CONCAT('R|', g.tag_name, '|', b.name)
 FROM dolt_tags g JOIN dolt_branches b ON b.hash = g.tag_hash;"

oracle "branches_left_join_tags" "$BRANCHY" \
"SELECT CONCAT('R|', b.name, '|', coalesce(g.tag_name, 'UNTAGGED'))
 FROM dolt_branches b LEFT JOIN dolt_tags g ON g.tag_hash = b.hash;"

oracle "branches_sharing_a_commit" "
$BRANCHY
SELECT dolt_branch('twin');
" \
"SELECT CONCAT('R|', b1.name, '|', b2.name)
 FROM dolt_branches b1 JOIN dolt_branches b2 ON b1.hash = b2.hash AND b1.name < b2.name;"

oracle "branch_tip_parent_messages" "$BRANCHY" \
"SELECT CONCAT('R|', b.name, '|', a.parent_index, '|', p.message)
 FROM dolt_branches b
 JOIN dolt_commit_ancestors a ON a.commit_hash = b.hash
 JOIN dolt_log p ON p.commit_hash = a.parent_hash;"

oracle "tag_reachable_from_branch_via_ancestors" "$BRANCHY" \
"SELECT CONCAT('R|', b.name, '|', g.tag_name)
 FROM dolt_branches b
 JOIN dolt_commit_ancestors a ON a.commit_hash = b.hash
 JOIN dolt_tags g ON g.tag_hash = a.parent_hash;"

oracle "branches_join_diff_tables_at_tip" "$BRANCHY" \
"SELECT CONCAT('R|', b.name, '|', d.table_name, '|', d.data_change)
 FROM dolt_branches b JOIN dolt_diff d ON d.commit_hash = b.hash;"

echo "--- status, workspace, blame, schemas, conflicts ---"

oracle "status_join_workspace_counts" "
$LINEAR
UPDATE t SET v = 1000 WHERE id = 1;
INSERT INTO t VALUES (7, 70);
SELECT dolt_add('t');
DELETE FROM u WHERE id = 2;
" \
"SELECT CONCAT('R|', s.table_name, '|', s.staged, '|', s.status, '|', w.n)
 FROM dolt_status s
 JOIN (SELECT 't' AS table_name, count(*) AS n FROM dolt_workspace_t
       UNION ALL SELECT 'u', count(*) FROM dolt_workspace_u) w
   ON w.table_name = s.table_name;"

oracle "workspace_join_current_rows" "
$LINEAR
UPDATE t SET v = 1000 WHERE id = 1;
INSERT INTO t VALUES (7, 70);
DELETE FROM t WHERE id = 3;
" \
"SELECT CONCAT('R|', w.diff_type, '|', coalesce(w.to_id, w.from_id), '|', coalesce(w.from_v, 'NULL'), '|', coalesce(w.to_v, 'NULL'), '|', coalesce(t.v, 'GONE'))
 FROM dolt_workspace_t w LEFT JOIN t ON t.id = coalesce(w.to_id, w.from_id);"

oracle "workspace_join_history_prior_value" "
$LINEAR
UPDATE t SET v = 1000 WHERE id = 1;
" \
"SELECT CONCAT('R|', w.to_id, '|', w.to_v, '|', h.v, '|', l.message)
 FROM dolt_workspace_t w
 JOIN dolt_history_t h ON h.id = w.to_id
 JOIN dolt_log l ON l.commit_hash = h.commit_hash
 WHERE w.diff_type = 'modified';"

oracle "blame_join_log_message_agrees" "$LINEAR" \
"SELECT CONCAT('R|', b.id, '|', l.message, '|', CASE WHEN b.message = l.message THEN 'same' ELSE 'DIFFERENT' END)
 FROM dolt_blame_t b JOIN dolt_log l ON l.commit_hash = b.\`commit\`;"

oracle "blame_join_history_value_is_current" "$BRANCHY" \
"SELECT CONCAT('R|', b.id, '|', h.v, '|', t.v, '|', b.message)
 FROM dolt_blame_t b
 JOIN dolt_history_t h ON h.commit_hash = b.\`commit\` AND h.id = b.id
 JOIN t ON t.id = b.id;"

oracle "blame_join_branches_tip_attribution" "$BRANCHY" \
"SELECT CONCAT('R|', b.id, '|', coalesce(br.name, 'INTERIOR'))
 FROM dolt_blame_t b LEFT JOIN dolt_branches br ON br.hash = b.\`commit\`;"

oracle "schemas_join_diff_and_log" "
$LINEAR
CREATE VIEW tv AS SELECT id, v FROM t WHERE v > 10;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_view');
CREATE VIEW uv AS SELECT id FROM u;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_view2');
" \
"SELECT CONCAT('R|', s.type, '|', s.name, '|', l.message)
 FROM dolt_schemas s
 JOIN dolt_diff d ON d.table_name = 'dolt_schemas'
 JOIN dolt_log l ON l.commit_hash = d.commit_hash;"

oracle "view_over_history_join" "
$LINEAR
CREATE VIEW t_at_c2 AS
  SELECT h.id, h.v FROM dolt_history_t h JOIN dolt_log l ON l.commit_hash = h.commit_hash WHERE l.message = 'c2';
" \
"SELECT CONCAT('R|', a.id, '|', a.v, '|', coalesce(t.v, 'GONE'))
 FROM t_at_c2 a LEFT JOIN t ON t.id = a.id;"

oracle "view_over_log_diff_join" "
$LINEAR
CREATE VIEW commit_tables AS
  SELECT l.message, d.table_name FROM dolt_log l JOIN dolt_diff d ON d.commit_hash = l.commit_hash;
" \
"SELECT CONCAT('R|', ct.message, '|', ct.table_name, '|', count(h.id))
 FROM commit_tables ct
 LEFT JOIN dolt_log l ON l.message = ct.message
 LEFT JOIN dolt_history_t h ON h.commit_hash = l.commit_hash AND ct.table_name = 't'
 GROUP BY ct.message, ct.table_name;"

oracle "cte_over_history_join" "$LINEAR" \
"WITH t_at_c2 AS (
   SELECT h.id, h.v FROM dolt_history_t h JOIN dolt_log l ON l.commit_hash = h.commit_hash WHERE l.message = 'c2')
 SELECT CONCAT('R|', a.id, '|', a.v, '|', coalesce(t.v, 'GONE'))
 FROM t_at_c2 a LEFT JOIN t ON t.id = a.id;"

oracle_in_merge "conflicts_join_merge_status_and_rows" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10), (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v = 100 WHERE id = 1;
UPDATE t SET v = 200 WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'featu');
SELECT dolt_checkout('main');
UPDATE t SET v = 1000 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', c.\`table\`, '|', c.num_conflicts, '|', m.is_merging, '|', m.source, '|', m.target, '|', m.unmerged_tables, '|', r.n)
 FROM dolt_conflicts c
 JOIN dolt_merge_status m
 JOIN (SELECT count(*) AS n FROM dolt_conflicts_t) r;"

oracle_in_merge "conflict_rows_join_current_and_branch_tip" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10), (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'featu');
SELECT dolt_checkout('main');
UPDATE t SET v = 1000 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', c.base_id, '|', c.base_v, '|', c.our_v, '|', c.their_v, '|', t.v, '|', b.latest_commit_message, '|', coalesce(l.message, 'NOT_IN_LOG'))
 FROM dolt_conflicts_t c
 JOIN t ON t.id = c.our_id
 JOIN dolt_branches b ON b.name = 'feat'
 LEFT JOIN dolt_log l ON l.commit_hash = b.hash;"

echo "--- AS OF joins ---"

oracle_as_of "as_of_head_minus_two_join_current" "$LINEAR" \
"SELECT CONCAT('R|', o.id, '|', coalesce(o.v, 'ABSENT'), '|', coalesce(n.v, 'ABSENT'))
 FROM dolt_at_t AS o LEFT JOIN t AS n ON n.id = o.id
 WHERE o.commit_ref = 'HEAD~2';" \
"SELECT CONCAT('R|', o.id, '|', coalesce(o.v, 'ABSENT'), '|', coalesce(n.v, 'ABSENT'))
 FROM t AS OF 'HEAD~2' AS o LEFT JOIN t AS n ON n.id = o.id;"

oracle_as_of "as_of_two_branches_join" "$BRANCHY" \
"SELECT CONCAT('R|', a.id, '|', a.v, '|', coalesce(b.v, 'ABSENT'))
 FROM dolt_at_t AS a LEFT JOIN dolt_at_t AS b ON b.id = a.id AND b.commit_ref = 'feat'
 WHERE a.commit_ref = 'side';" \
"SELECT CONCAT('R|', a.id, '|', a.v, '|', coalesce(b.v, 'ABSENT'))
 FROM t AS OF 'side' AS a LEFT JOIN t AS OF 'feat' AS b ON b.id = a.id;"

oracle_as_of "as_of_tag_join_history" "$BRANCHY" \
"SELECT CONCAT('R|', a.id, '|', a.v, '|', h.v, '|', l.message)
 FROM dolt_at_t AS a
 JOIN dolt_history_t h ON h.id = a.id AND h.v <> a.v
 JOIN dolt_log l ON l.commit_hash = h.commit_hash
 WHERE a.commit_ref = 'v1';" \
"SELECT CONCAT('R|', a.id, '|', a.v, '|', h.v, '|', l.message)
 FROM t AS OF 'v1' AS a
 JOIN dolt_history_t h ON h.id = a.id AND h.v <> a.v
 JOIN dolt_log l ON l.commit_hash = h.commit_hash;"

oracle_as_of "as_of_three_way_compare" "$BRANCHY" \
"SELECT CONCAT('R|', base.id, '|', base.v, '|', coalesce(s.v, 'ABSENT'), '|', coalesce(f.v, 'ABSENT'), '|', cur.v)
 FROM dolt_at_t AS base
 LEFT JOIN dolt_at_t AS s ON s.id = base.id AND s.commit_ref = 'side'
 LEFT JOIN dolt_at_t AS f ON f.id = base.id AND f.commit_ref = 'feat'
 JOIN t AS cur ON cur.id = base.id
 WHERE base.commit_ref = 'v1';" \
"SELECT CONCAT('R|', base.id, '|', base.v, '|', coalesce(s.v, 'ABSENT'), '|', coalesce(f.v, 'ABSENT'), '|', cur.v)
 FROM t AS OF 'v1' AS base
 LEFT JOIN t AS OF 'side' AS s ON s.id = base.id
 LEFT JOIN t AS OF 'feat' AS f ON f.id = base.id
 JOIN t AS cur ON cur.id = base.id;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
