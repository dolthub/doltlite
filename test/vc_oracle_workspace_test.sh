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

translate_for_dolt() {
  sed -E '
    s/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g
    s/"dolt_diff_([^"]+)"\(([^)]*)\)/dolt_diff(\2, "\1")/g
    s/`dolt_diff_([^`]+)`\(([^)]*)\)/dolt_diff(\2, '"'"'\1'"'"')/g
    s/dolt_diff_([a-zA-Z0-9_]+)\(([^)]*)\)/dolt_diff(\2, "\1")/g
  '
}

oracle() {
  local name="$1" setup="$2" query="$3" allow_empty="${4:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^R|' | sort)

  local dolt_setup dolt_query
  dolt_setup=$(echo "$setup" | translate_for_dolt)
  dolt_query=$(echo "$query" | translate_for_dolt)

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      echo "$dolt_setup"
      echo "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

# Both engines must reject without crashing.
oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?

  local dolt_setup
  dolt_setup=$(echo "$setup" | translate_for_dolt)
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
    if [ -s "$dir/dl.err" ]; then
      echo "    doltlite err:"; sed 's/^/      /' "$dir/dl.err"
    fi
    if [ -s "$dir/dt.err" ]; then
      echo "    dolt err:"; sed 's/^/      /' "$dir/dt.err"
    fi
  fi
}

echo "=== Version Control Oracle Tests: dolt_workspace tables ==="
echo ""

BASE="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT, confidence INT);
INSERT INTO t VALUES(1,10,0),(2,20,0),(3,30,0),(4,40,0);
SELECT dolt_commit('-A','-m','seed');
"

oracle "workspace_read_modified_rows" "$BASE
UPDATE t SET v=v+100, confidence=id-3;
" "SELECT CONCAT('R|', staged, '|', diff_type, '|', to_id, '|', to_v, '|', from_v)
       FROM dolt_workspace_t
      ORDER BY to_id;"

oracle "workspace_clean_table_empty" "$BASE" \
  "SELECT CONCAT('R|', staged, '|', diff_type, '|', to_id)
     FROM dolt_workspace_t
    ORDER BY to_id;" \
  "EXPECT_EMPTY"

oracle "workspace_read_added_rows" "$BASE
INSERT INTO t VALUES(5,50,1),(6,60,-1);
" "SELECT CONCAT('R|', staged, '|', diff_type, '|', to_id, '|', to_v, '|', to_confidence)
       FROM dolt_workspace_t
      ORDER BY to_id;"

oracle "workspace_read_removed_rows" "$BASE
DELETE FROM t WHERE id IN (1,3);
" "SELECT CONCAT('R|', staged, '|', diff_type, '|', IFNULL(from_id,''), '|', IFNULL(from_v,''))
       FROM dolt_workspace_t
      ORDER BY from_id;"

oracle "workspace_read_mixed_diff_types" "$BASE
UPDATE t SET v=200 WHERE id=2;
DELETE FROM t WHERE id=3;
INSERT INTO t VALUES(5,50,1);
" "SELECT CONCAT('R|', diff_type, '|', IFNULL(to_id,''), '|', IFNULL(from_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_v,''))
       FROM dolt_workspace_t
      ORDER BY diff_type, IFNULL(to_id, from_id);"

oracle "stage_positive_confidence_only" "$BASE
UPDATE t SET v=v+100, confidence=id-3;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_confidence > from_confidence;
SELECT dolt_commit('-m','partial');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;"

oracle "stage_even_ids_only" "$BASE
UPDATE t SET v=v+100;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id % 2 = 0;
SELECT dolt_commit('-m','even_ids');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;"

oracle "stage_by_from_value" "$BASE
UPDATE t SET v=v+100;
UPDATE dolt_workspace_t SET staged=TRUE WHERE from_v >= 30;
SELECT dolt_commit('-m','from_value');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;"

oracle "stage_all_workspace_rows" "$BASE
UPDATE t SET v=v+100;
UPDATE dolt_workspace_t SET staged=TRUE;
SELECT dolt_commit('-m','all_workspace');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;" \
  "EXPECT_EMPTY"

oracle "stage_modified_only_from_mixed_changes" "$BASE
UPDATE t SET v=200 WHERE id=2;
DELETE FROM t WHERE id=3;
INSERT INTO t VALUES(5,50,1);
UPDATE dolt_workspace_t SET staged=TRUE WHERE diff_type='modified';
SELECT dolt_commit('-m','modified_only');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', diff_type, '|', IFNULL(to_id,''), '|', IFNULL(from_id,''))
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY diff_type, IFNULL(to_id, from_id);"

oracle "stage_specific_insert" "$BASE
INSERT INTO t VALUES(5,50,1),(6,60,-1);
UPDATE dolt_workspace_t SET staged=TRUE WHERE diff_type='added' AND to_confidence > 0;
SELECT dolt_commit('-m','insert_one');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;"

oracle "stage_multiple_inserts_by_id_range" "$BASE
INSERT INTO t VALUES(5,50,1),(6,60,1),(7,70,1);
UPDATE dolt_workspace_t SET staged=TRUE WHERE diff_type='added' AND to_id BETWEEN 5 AND 6;
SELECT dolt_commit('-m','insert_range');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;"

oracle "stage_insert_then_unstage_one" "$BASE
INSERT INTO t VALUES(5,50,1),(6,60,1);
UPDATE dolt_workspace_t SET staged=TRUE WHERE diff_type='added';
UPDATE dolt_workspace_t SET staged=FALSE WHERE to_id=6;
SELECT dolt_commit('-m','insert_unstage_one');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;"

oracle "stage_specific_delete" "$BASE
DELETE FROM t WHERE id IN (1,2);
UPDATE dolt_workspace_t SET staged=TRUE WHERE diff_type='removed' AND from_id=1;
SELECT dolt_commit('-m','delete_one');
" "SELECT CONCAT('R|COMMITTED|', from_id, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY from_id;
SELECT CONCAT('R|WORKING_DIFF|', from_id, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY from_id;"

oracle "stage_multiple_deletes_by_predicate" "$BASE
DELETE FROM t WHERE id IN (1,2,4);
UPDATE dolt_workspace_t SET staged=TRUE WHERE diff_type='removed' AND from_v <= 20;
SELECT dolt_commit('-m','delete_predicate');
" "SELECT CONCAT('R|COMMITTED|', from_id, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY from_id;
SELECT CONCAT('R|WORKING_DIFF|', from_id, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY from_id;"

oracle "stage_delete_then_unstage_one" "$BASE
DELETE FROM t WHERE id IN (1,2,3);
UPDATE dolt_workspace_t SET staged=TRUE WHERE diff_type='removed';
UPDATE dolt_workspace_t SET staged=FALSE WHERE from_id=2;
SELECT dolt_commit('-m','delete_unstage_one');
" "SELECT CONCAT('R|COMMITTED|', from_id, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY from_id;
SELECT CONCAT('R|WORKING_DIFF|', from_id, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY from_id;"

oracle "unstage_one_modified_row" "$BASE
UPDATE t SET v=v+100;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id IN (1,2,3);
UPDATE dolt_workspace_t SET staged=FALSE WHERE to_id=2;
SELECT dolt_commit('-m','partial_unstage');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;"

oracle "unstage_all_after_stage_all" "$BASE
UPDATE t SET v=v+100;
UPDATE dolt_workspace_t SET staged=TRUE;
UPDATE dolt_workspace_t SET staged=FALSE;
" "SELECT CONCAT('R|', staged, '|', diff_type, '|', to_id, '|', to_v, '|', from_v)
       FROM dolt_workspace_t
      ORDER BY to_id;"

oracle "restage_after_unstage" "$BASE
UPDATE t SET v=v+100;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id IN (1,2);
UPDATE dolt_workspace_t SET staged=FALSE WHERE to_id=1;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=1;
SELECT dolt_commit('-m','restage');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;"

oracle "workspace_status_before_commit" "$BASE
UPDATE t SET v=v+100;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id IN (1,3);
" "SELECT CONCAT('R|WS|', staged, '|', to_id, '|', to_v, '|', from_v)
       FROM dolt_workspace_t
      ORDER BY staged DESC, to_id;
SELECT CONCAT('R|STATUS|', table_name, '|', staged, '|', status)
       FROM dolt_status
      ORDER BY staged, table_name;"

oracle "workspace_after_partial_commit_shows_remaining" "$BASE
UPDATE t SET v=v+100;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=1;
SELECT dolt_commit('-m','one');
" "SELECT CONCAT('R|WS|', staged, '|', to_id, '|', to_v, '|', from_v)
       FROM dolt_workspace_t
      ORDER BY to_id;"

oracle "second_round_partial_commit" "$BASE
UPDATE t SET v=v+100;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id IN (1,2);
SELECT dolt_commit('-m','round1');
UPDATE t SET v=v+1000 WHERE id IN (2,3,4);
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id IN (2,4);
SELECT dolt_commit('-m','round2');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;"

oracle "working_advances_past_staged_row" "$BASE
UPDATE t SET v=100 WHERE id=1;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=1;
UPDATE t SET v=200 WHERE id=1;
" "SELECT CONCAT('R|WS|', staged, '|', to_id, '|', to_v, '|', from_v)
       FROM dolt_workspace_t
      ORDER BY staged DESC, to_id;"

oracle "commit_staged_when_working_advances" "$BASE
UPDATE t SET v=100 WHERE id=1;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=1;
UPDATE t SET v=200 WHERE id=1;
SELECT dolt_commit('-m','staged_old_value');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;"

oracle "multi_table_isolation_stage_t_only" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE u(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10),(2,20);
INSERT INTO u VALUES(1,100),(2,200);
SELECT dolt_commit('-A','-m','seed');
UPDATE t SET v=v+1;
UPDATE u SET v=v+1;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=1;
SELECT dolt_commit('-m','stage_t_one');
" "SELECT CONCAT('R|T_COMMITTED|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|T_WORKING|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;
SELECT CONCAT('R|U_WORKING|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_u('HEAD', 'WORKING') ORDER BY to_id;"

oracle "multi_table_stage_each_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE u(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10),(2,20);
INSERT INTO u VALUES(1,100),(2,200);
SELECT dolt_commit('-A','-m','seed');
UPDATE t SET v=v+1;
UPDATE u SET v=v+1;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=2;
UPDATE dolt_workspace_u SET staged=TRUE WHERE to_id=1;
SELECT dolt_commit('-m','stage_each');
" "SELECT CONCAT('R|T_COMMITTED|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|U_COMMITTED|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_u('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|T_WORKING|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_t('HEAD', 'WORKING') ORDER BY to_id;
SELECT CONCAT('R|U_WORKING|', to_id, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_u('HEAD', 'WORKING') ORDER BY to_id;"

oracle "text_and_null_values" "
CREATE TABLE s(id INTEGER PRIMARY KEY, v TEXT, note TEXT);
INSERT INTO s VALUES(1,'a',NULL),(2,'b','old'),(3,'c',NULL);
SELECT dolt_commit('-A','-m','seed');
UPDATE s SET v=upper(v), note=CASE id WHEN 1 THEN 'new' WHEN 2 THEN NULL ELSE 'keep' END;
UPDATE dolt_workspace_s SET staged=TRUE WHERE from_note IS NULL AND to_note IS NOT NULL;
SELECT dolt_commit('-m','text_null');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_v, '|', IFNULL(to_note,'NULL'), '|', IFNULL(from_note,'NULL'), '|', diff_type)
  FROM dolt_diff_s('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_v, '|', IFNULL(to_note,'NULL'), '|', IFNULL(from_note,'NULL'), '|', diff_type)
  FROM dolt_diff_s('HEAD', 'WORKING') ORDER BY to_id;"

oracle "quoted_table_and_column_names" "
CREATE TABLE \`odd table\`(id INTEGER PRIMARY KEY, \`select\` INT, \`two words\` TEXT);
INSERT INTO \`odd table\` VALUES(1,10,'a'),(2,20,'b');
SELECT dolt_commit('-A','-m','seed');
UPDATE \`odd table\` SET \`select\`=\`select\`+100, \`two words\`=upper(\`two words\`);
UPDATE \`dolt_workspace_odd table\` SET staged=TRUE WHERE to_id=2;
SELECT dolt_commit('-m','quoted');
" "SELECT CONCAT('R|COMMITTED|', to_id, '|', to_select, '|', from_select, '|', diff_type)
  FROM \`dolt_diff_odd table\`('HEAD~1', 'HEAD') ORDER BY to_id;
SELECT CONCAT('R|WORKING_DIFF|', to_id, '|', to_select, '|', from_select, '|', diff_type)
  FROM \`dolt_diff_odd table\`('HEAD', 'WORKING') ORDER BY to_id;"

oracle "composite_pk_stage_subset" "
CREATE TABLE m(a INT, b INT, v TEXT, score INT, PRIMARY KEY(a,b));
INSERT INTO m VALUES(1,1,'one',0),(1,2,'two',0),(2,1,'three',0);
SELECT dolt_commit('-A','-m','seed');
UPDATE m SET v=upper(v), score=a+b;
UPDATE dolt_workspace_m SET staged=TRUE WHERE to_score >= 3;
SELECT dolt_commit('-m','partial_composite');
" "SELECT CONCAT('R|COMMITTED|', to_a, '|', to_b, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_m('HEAD~1', 'HEAD') ORDER BY to_a,to_b;
SELECT CONCAT('R|WORKING_DIFF|', to_a, '|', to_b, '|', to_v, '|', from_v, '|', diff_type)
  FROM dolt_diff_m('HEAD', 'WORKING') ORDER BY to_a,to_b;"

oracle "composite_pk_stage_insert_and_update" "
CREATE TABLE m(a INT, b INT, v TEXT, score INT, PRIMARY KEY(a,b));
INSERT INTO m VALUES(1,1,'one',0),(1,2,'two',0);
SELECT dolt_commit('-A','-m','seed');
UPDATE m SET v='ONE', score=10 WHERE a=1 AND b=1;
INSERT INTO m VALUES(2,1,'three',3),(2,2,'four',4);
UPDATE dolt_workspace_m SET staged=TRUE WHERE to_a=2 OR to_score=10;
SELECT dolt_commit('-m','composite_insert_update');
" "SELECT CONCAT('R|COMMITTED|', diff_type, '|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''))
  FROM dolt_diff_m('HEAD~1', 'HEAD') ORDER BY diff_type, IFNULL(to_a,from_a), IFNULL(to_b,from_b);
SELECT CONCAT('R|WORKING_DIFF|', diff_type, '|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''))
  FROM dolt_diff_m('HEAD', 'WORKING') ORDER BY diff_type, IFNULL(to_a,from_a), IFNULL(to_b,from_b);"

oracle "composite_pk_delete_subset" "
CREATE TABLE m(a INT, b INT, v TEXT, PRIMARY KEY(a,b));
INSERT INTO m VALUES(1,1,'one'),(1,2,'two'),(2,1,'three');
SELECT dolt_commit('-A','-m','seed');
DELETE FROM m WHERE a=1 OR b=1;
UPDATE dolt_workspace_m SET staged=TRUE WHERE from_a=1 AND from_b=2;
SELECT dolt_commit('-m','composite_delete_subset');
" "SELECT CONCAT('R|COMMITTED|', from_a, '|', from_b, '|', from_v, '|', diff_type)
  FROM dolt_diff_m('HEAD~1', 'HEAD') ORDER BY from_a,from_b;
SELECT CONCAT('R|WORKING_DIFF|', from_a, '|', from_b, '|', from_v, '|', diff_type)
  FROM dolt_diff_m('HEAD', 'WORKING') ORDER BY from_a,from_b;"

IDX="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT, confidence INT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,10,0),(2,20,0),(3,30,0),(4,40,0);
SELECT dolt_commit('-A','-m','seed');
"

oracle "idx_stage_modified_one" "$IDX
UPDATE t SET v=120 WHERE id=2;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=2;
SELECT dolt_commit('-m','mod2');
" "SELECT CONCAT('R|C|', to_id, '|', to_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1','HEAD') ORDER BY to_id;
SELECT CONCAT('R|IDX|', id, '|', v) FROM t WHERE v=120 ORDER BY id;
SELECT CONCAT('R|OLD|', id) FROM t WHERE v=20 ORDER BY id;"

oracle "idx_stage_added_one" "$IDX
INSERT INTO t VALUES(5,50,1),(6,60,-1);
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=5;
SELECT dolt_commit('-m','add5');
" "SELECT CONCAT('R|C|', to_id, '|', to_v, '|', diff_type)
  FROM dolt_diff_t('HEAD~1','HEAD') ORDER BY to_id;
SELECT CONCAT('R|IDX|', id, '|', v) FROM t WHERE v=50 ORDER BY id;"

oracle "idx_stage_deleted_one" "$IDX
DELETE FROM t WHERE id=3;
UPDATE dolt_workspace_t SET staged=TRUE WHERE from_id=3;
SELECT dolt_commit('-m','del3');
" "SELECT CONCAT('R|C|', IFNULL(from_id,''), '|', diff_type)
  FROM dolt_diff_t('HEAD~1','HEAD') ORDER BY from_id;
SELECT CONCAT('R|IDX|', id) FROM t WHERE v=30 ORDER BY id;"

oracle "idx_modify_indexed_col_moves_entry" "$IDX
UPDATE t SET v=999 WHERE id=1;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=1;
SELECT dolt_commit('-m','mv1');
DELETE FROM t WHERE id IN (2,3,4);
" "SELECT CONCAT('R|NEW|', id, '|', v) FROM t WHERE v=999 ORDER BY id;
SELECT CONCAT('R|GONE|', id) FROM t WHERE v=10 ORDER BY id;"

oracle "idx_stage_subset_orderby_index" "$IDX
UPDATE t SET v=v+1000 WHERE id IN (1,3);
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id IN (1,3);
SELECT dolt_commit('-m','subset');
" "SELECT CONCAT('R|ORD|', id, '|', v) FROM t ORDER BY v, id;"

oracle "idx_stage_then_unstage" "$IDX
UPDATE t SET v=777 WHERE id=2;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=2;
UPDATE dolt_workspace_t SET staged=FALSE WHERE to_id=2;
" "SELECT CONCAT('R|W|', staged, '|', diff_type, '|', to_id, '|', to_v)
  FROM dolt_workspace_t ORDER BY to_id;
SELECT CONCAT('R|IDX|', id, '|', v) FROM t WHERE v=777 ORDER BY id;"

oracle "idx_unique_stage_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
CREATE UNIQUE INDEX uab ON t(a,b);
INSERT INTO t VALUES(1,'p','q'),(2,'r','s');
SELECT dolt_commit('-A','-m','seed');
INSERT INTO t VALUES(3,'x','y');
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=3;
SELECT dolt_commit('-m','add3');
" "SELECT CONCAT('R|C|', to_id, '|', to_a, '|', to_b)
  FROM dolt_diff_t('HEAD~1','HEAD') ORDER BY to_id;
SELECT CONCAT('R|IDX|', id) FROM t WHERE a='x' AND b='y';"

oracle "idx_multicol_stage_modify" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
CREATE INDEX iab ON t(a,b);
INSERT INTO t VALUES(1,'p','q'),(2,'r','s');
SELECT dolt_commit('-A','-m','seed');
UPDATE t SET b='Z' WHERE id=1;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=1;
SELECT dolt_commit('-m','mod1');
DELETE FROM t WHERE id=2;
" "SELECT CONCAT('R|NEW|', id, '|', b) FROM t WHERE a='p' AND b='Z' ORDER BY id;
SELECT CONCAT('R|GONE|', id) FROM t WHERE a='p' AND b='q' ORDER BY id;"

oracle "idx_ignore_present_stage_indexed" "
INSERT INTO dolt_ignore VALUES('gen_%', 1);
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_commit('-A','-m','seed');
CREATE TABLE gen_tmp(id INTEGER PRIMARY KEY, x INT);
INSERT INTO gen_tmp VALUES(1,1);
UPDATE t SET v=200 WHERE id=1;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=1;
SELECT dolt_commit('-m','stage t only');
" "SELECT CONCAT('R|C|', to_id, '|', to_v) FROM dolt_diff_t('HEAD~1','HEAD') ORDER BY to_id;
SELECT CONCAT('R|IDX|', id, '|', v) FROM t WHERE v=200 ORDER BY id;"

oracle "idx_ignore_addall_skips_ignored_indexed" "
INSERT INTO dolt_ignore VALUES('tmp_%', 1);
CREATE TABLE keep(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX kv ON keep(v);
CREATE TABLE tmp_idx(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX tv ON tmp_idx(v);
INSERT INTO keep VALUES(1,10),(2,20);
INSERT INTO tmp_idx VALUES(1,99);
SELECT dolt_commit('-A','-m','seed');
" "SELECT CONCAT('R|KEEP|', to_id, '|', to_v) FROM dolt_diff_keep('HEAD~1','HEAD') ORDER BY to_id;
SELECT CONCAT('R|IDX|', id, '|', v) FROM keep WHERE v=20 ORDER BY id;"

oracle "idx_range_query_after_stage" "$IDX
UPDATE t SET v=15 WHERE id=1;
UPDATE t SET v=25 WHERE id=2;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id IN (1,2);
SELECT dolt_commit('-m','range');
" "SELECT CONCAT('R|RNG|', id, '|', v) FROM t WHERE v BETWEEN 15 AND 30 ORDER BY v, id;"



oracle "workspace_delete_discards_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
SELECT dolt_commit('-A','-m','seed empty');
INSERT INTO t VALUES(42,42),(43,43);
DELETE FROM dolt_workspace_t WHERE to_id=42;
" "SELECT CONCAT('R|DATA|', id, '|', val) FROM t ORDER BY id;
SELECT CONCAT('R|WS|', staged, '|', diff_type, '|', IFNULL(to_id,''), '|', IFNULL(to_val,''))
  FROM dolt_workspace_t ORDER BY to_id;"

oracle "workspace_delete_restores_removed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(42,42),(43,43);
SELECT dolt_commit('-A','-m','seed');
DELETE FROM t;
DELETE FROM dolt_workspace_t WHERE from_id=42;
" "SELECT CONCAT('R|DATA|', id, '|', val) FROM t ORDER BY id;
SELECT CONCAT('R|WS|', staged, '|', diff_type, '|', IFNULL(from_id,''), '|', IFNULL(from_val,''))
  FROM dolt_workspace_t ORDER BY from_id;"

oracle "workspace_delete_reverts_modify" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(42,42),(43,43);
SELECT dolt_commit('-A','-m','seed');
UPDATE t SET val=val*2;
DELETE FROM dolt_workspace_t WHERE to_id=42;
" "SELECT CONCAT('R|DATA|', id, '|', val) FROM t ORDER BY id;
SELECT CONCAT('R|WS|', staged, '|', diff_type, '|', to_id, '|', to_val, '|', from_val)
  FROM dolt_workspace_t ORDER BY to_id;"

oracle "workspace_delete_after_unstage" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
SELECT dolt_commit('-A','-m','seed empty');
INSERT INTO t VALUES(42,42),(43,43);
SELECT dolt_add('t');
UPDATE dolt_workspace_t SET staged=FALSE WHERE to_id=42;
DELETE FROM dolt_workspace_t WHERE to_id=42;
" "SELECT CONCAT('R|DATA|', id, '|', val) FROM t ORDER BY id;
SELECT CONCAT('R|WS|', staged, '|', diff_type, '|', to_id, '|', to_val)
  FROM dolt_workspace_t ORDER BY to_id;"

oracle "workspace_delete_all_unstaged_modifies" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_commit('-A','-m','seed');
UPDATE t SET val=val+1;
DELETE FROM dolt_workspace_t;
" "SELECT CONCAT('R|DATA|', id, '|', val) FROM t ORDER BY id;
SELECT CONCAT('R|WS|', count(*)) FROM dolt_workspace_t;"

oracle "workspace_delete_index_reverts_modify" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX idx_t_v ON t(v);
INSERT INTO t VALUES(1,1),(2,2);
SELECT dolt_commit('-A','-m','seed');
UPDATE t SET v=99 WHERE id=1;
DELETE FROM dolt_workspace_t WHERE to_id=1;
" "SELECT CONCAT('R|DATA|', id, '|', v) FROM t ORDER BY id;
SELECT CONCAT('R|IDX_OLD|', id, '|', v) FROM t WHERE v=1 ORDER BY id;
SELECT CONCAT('R|IDX_NEW|', id, '|', v) FROM t WHERE v=99 ORDER BY id;
SELECT CONCAT('R|WS|', count(*)) FROM dolt_workspace_t;"

oracle "workspace_delete_mixed_keep_other_types" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_commit('-A','-m','seed');
UPDATE t SET val=200 WHERE id=2;
DELETE FROM t WHERE id=3;
INSERT INTO t VALUES(5,50);
DELETE FROM dolt_workspace_t WHERE diff_type='modified';
" "SELECT CONCAT('R|DATA|', id, '|', val) FROM t ORDER BY id;
SELECT CONCAT('R|WS|', staged, '|', diff_type, '|', IFNULL(to_id,''), '|', IFNULL(from_id,''))
  FROM dolt_workspace_t
 ORDER BY diff_type, IFNULL(to_id, from_id);"

oracle_error "workspace_delete_staged_rejected" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
SELECT dolt_commit('-A','-m','seed empty');
INSERT INTO t VALUES(42,42);
SELECT dolt_add('t');
DELETE FROM dolt_workspace_t WHERE to_id=42;
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
