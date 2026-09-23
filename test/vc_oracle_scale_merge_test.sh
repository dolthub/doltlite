#!/bin/bash
# 40k-row tables with ~100-byte rows build prolly trees of three or more
# levels, so merge, cherry-pick and diff walk internal nodes, whole subtrees
# and leaf boundaries rather than a single leaf. Compare the results vs Dolt.

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

translate_for_dolt() {
  sed -E '
    s/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g
    s/dolt_diff_(stat|summary)([^a-zA-Z0-9_])/@@DOLT_DIFF_\1@@\2/g
    s/dolt_diff_([a-zA-Z0-9_]+)\(([^)]*)\)/dolt_diff(\2, "\1")/g
    s/@@DOLT_DIFF_(stat|summary)@@/dolt_diff_\1/g
  '
}

# Setup runs once per engine; every query emits R|<label>|... rows and each
# label is compared on its own.
oracle_scenario() {
  local name="$1" setup="$2" queries="$3"
  local dir="$TMPROOT/$name"
  local dl_all dt_all label labels
  mkdir -p "$dir/dl" "$dir/dt"

  dl_all=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$queries" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' | grep '^R|' | sort)

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      echo "$setup" | translate_for_dolt
      echo "$queries" | translate_for_dolt
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_all=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  labels=$(printf '%s\n' "$queries" | grep -o "'R|[a-z_0-9]*|'" \
           | sed -E "s/'R\|([a-z_0-9]*)\|'/\1/" | sort -u)
  for label in $labels; do
    vc_oracle_assert_match "${name}_${label}" \
      "$(printf '%s\n' "$dl_all" | grep "^R|$label|")" \
      "$(printf '%s\n' "$dt_all" | grep "^R|$label|")"
  done
}

echo "=== Version Control Oracle Tests: merge and diff over multi-level trees ==="
echo ""

PAD="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnop"
SEQ="WITH RECURSIVE s(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM s WHERE n < 199)"

SEED="
CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT, v VARCHAR(200));
CREATE INDEX t_a ON t(a);
INSERT INTO t $SEQ
  SELECT (x.n*200 + y.n)*10, (x.n*200 + y.n) % 1000, x.n*200 + y.n,
         CONCAT('row-', x.n*200 + y.n, '-', '$PAD')
  FROM s x, s y;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed'); SELECT dolt_tag('seed');
"

# Rows each side deletes in bulk; the other side's spread edits avoid them so
# the merge stays clean.
KEEP="b NOT BETWEEN 10000 AND 12999 AND b NOT BETWEEN 30000 AND 31499"

FEAT_EDITS="
UPDATE t SET a = a + 1 WHERE b % 97 = 0 AND $KEEP;
DELETE FROM t WHERE b BETWEEN 10000 AND 12999;
INSERT INTO t SELECT id + 5, a, b, CONCAT(v, '-gap') FROM t WHERE b % 151 = 3;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
UPDATE t SET v = CONCAT(v, '-f2') WHERE b % 173 = 7 AND $KEEP;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f2'); SELECT dolt_tag('feat_tip');
"

MAIN_EDITS="
UPDATE t SET b = b + 1000000 WHERE b % 97 = 0 AND $KEEP;
DELETE FROM t WHERE b BETWEEN 30000 AND 31499;
INSERT INTO t $SEQ
  SELECT 1000000 + x.n*200 + y.n, 7, x.n*200 + y.n, CONCAT('tail-', x.n*200 + y.n)
  FROM s x, s y WHERE x.n < 5;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm1'); SELECT dolt_tag('main_tip');
"

TABLE_QUERIES="
SELECT CONCAT('R|agg|', count(*), '|', SUM(id), '|', SUM(a), '|', SUM(b), '|', SUM(LENGTH(v))) FROM t;
SELECT CONCAT('R|sample|', id, '|', a, '|', b, '|', v) FROM t WHERE id IN (0, 35, 50, 70, 99990, 100030, 119990, 130000, 250070, 299990, 315010, 399990, 1000000, 1000999);
SELECT CONCAT('R|index_range|', count(*), '|', SUM(id), '|', SUM(b)) FROM t WHERE a BETWEEN 100 AND 120;
"

DIFF_QUERIES="
SELECT CONCAT('R|diff_seed_feat|', diff_type, '|', count(*), '|', SUM(COALESCE(to_id, 0)), '|', SUM(COALESCE(from_id, 0)), '|', SUM(COALESCE(to_a, 0))) FROM dolt_diff_t('seed', 'feat_tip') GROUP BY diff_type;
SELECT CONCAT('R|diff_main_feat|', diff_type, '|', count(*), '|', SUM(COALESCE(to_id, 0)), '|', SUM(COALESCE(from_id, 0)), '|', SUM(COALESCE(to_b, 0)), '|', SUM(COALESCE(from_b, 0))) FROM dolt_diff_t('main_tip', 'feat_tip') GROUP BY diff_type;
SELECT CONCAT('R|diff_merge|', diff_type, '|', count(*), '|', SUM(COALESCE(to_id, 0)), '|', SUM(COALESCE(from_id, 0)), '|', SUM(LENGTH(COALESCE(to_v, '')))) FROM dolt_diff_t('main_tip', 'main') GROUP BY diff_type;
SELECT CONCAT('R|stat_seed_feat|', table_name, '|', rows_unmodified, '|', rows_added, '|', rows_deleted, '|', rows_modified, '|', cells_added, '|', cells_deleted, '|', cells_modified, '|', old_row_count, '|', new_row_count) FROM dolt_diff_stat('seed', 'feat_tip', 't');
SELECT CONCAT('R|stat_merge|', table_name, '|', rows_unmodified, '|', rows_added, '|', rows_deleted, '|', rows_modified, '|', cells_added, '|', cells_deleted, '|', cells_modified, '|', old_row_count, '|', new_row_count) FROM dolt_diff_stat('main_tip', 'main', 't');
SELECT CONCAT('R|summary_seed_main|', from_table_name, '|', to_table_name, '|', diff_type, '|', data_change, '|', schema_change) FROM dolt_diff_summary('seed', 'main');
"

oracle_scenario "merge" "
$SEED
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
$FEAT_EDITS
SELECT dolt_checkout('main');
$MAIN_EDITS
SELECT dolt_merge('feat');
" "$TABLE_QUERIES
$DIFF_QUERIES"

oracle_scenario "merge_into_feat" "
$SEED
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
$FEAT_EDITS
SELECT dolt_checkout('main');
$MAIN_EDITS
SELECT dolt_checkout('feat');
SELECT dolt_merge('main');
" "$TABLE_QUERIES
SELECT CONCAT('R|stat_merge|', table_name, '|', rows_unmodified, '|', rows_added, '|', rows_deleted, '|', rows_modified, '|', cells_added, '|', cells_deleted, '|', cells_modified, '|', old_row_count, '|', new_row_count) FROM dolt_diff_stat('feat_tip', 'feat', 't');"

oracle_scenario "cherry_pick" "
$SEED
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
$FEAT_EDITS
SELECT dolt_checkout('main');
$MAIN_EDITS
SELECT dolt_cherry_pick('feat~1');
SELECT dolt_cherry_pick('feat');
" "$TABLE_QUERIES
SELECT CONCAT('R|stat_picked|', table_name, '|', rows_unmodified, '|', rows_added, '|', rows_deleted, '|', rows_modified, '|', cells_added, '|', cells_deleted, '|', cells_modified, '|', old_row_count, '|', new_row_count) FROM dolt_diff_stat('main_tip', 'main', 't');
SELECT CONCAT('R|diff_picked|', diff_type, '|', count(*), '|', SUM(COALESCE(to_id, 0)), '|', SUM(COALESCE(from_id, 0)), '|', SUM(COALESCE(to_a, 0))) FROM dolt_diff_t('main_tip', 'main') GROUP BY diff_type;"

SCHEMA_FEAT_EDITS="
ALTER TABLE t ADD COLUMN c INT DEFAULT 7;
UPDATE t SET c = b % 13 WHERE b % 89 = 4 AND $KEEP;
DELETE FROM t WHERE b BETWEEN 10000 AND 12999;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1'); SELECT dolt_tag('feat_tip');
"

SCHEMA_QUERIES="
SELECT CONCAT('R|agg|', count(*), '|', SUM(id), '|', SUM(a), '|', SUM(b), '|', SUM(LENGTH(v)), '|', SUM(COALESCE(c, -1)), '|', SUM(c = 7)) FROM t;
SELECT CONCAT('R|sample|', id, '|', a, '|', b, '|', COALESCE(c, 'null'), '|', v) FROM t WHERE id IN (0, 35, 40, 70, 99990, 130000, 250070, 315010, 399990, 1000000, 1000999);
SELECT CONCAT('R|index_range|', count(*), '|', SUM(id), '|', SUM(COALESCE(c, -1))) FROM t WHERE a BETWEEN 100 AND 120;
SELECT CONCAT('R|summary_seed_main|', from_table_name, '|', to_table_name, '|', diff_type, '|', data_change, '|', schema_change) FROM dolt_diff_summary('seed', 'main');
"

oracle_scenario "schema_change_merge" "
$SEED
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
$SCHEMA_FEAT_EDITS
SELECT dolt_checkout('main');
$MAIN_EDITS
SELECT dolt_merge('feat');
" "$SCHEMA_QUERIES"

oracle_scenario "schema_change_merge_into_feat" "
$SEED
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
$SCHEMA_FEAT_EDITS
SELECT dolt_checkout('main');
$MAIN_EDITS
SELECT dolt_checkout('feat');
SELECT dolt_merge('main');
" "$SCHEMA_QUERIES"

vc_oracle_finish
