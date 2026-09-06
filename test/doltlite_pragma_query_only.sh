#!/bin/bash
# PRAGMA query_only and immutable=1 must refuse version-control writes, not only DML.
DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== PRAGMA query_only / immutable=1 refuse version-control writes ==="
echo ""

ROOT=$(mktemp -d /tmp/dl_query_only_XXXXXX)
trap 'rm -rf "$ROOT"' EXIT
DB="$ROOT/qo.db"
RO="attempt to write a readonly database"

run_test "seed" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT length(dolt_commit('-Am','init'));
SELECT dolt_branch('b');
SELECT dolt_checkout('b');
INSERT INTO t VALUES(2,'b');
SELECT length(dolt_commit('-am','on b'));
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'dirty');
" "40
0
0
40
0" "$DB"

QO="PRAGMA query_only=1;"

run_test_match "qo_insert_refused" "$QO INSERT INTO t VALUES(4,'x');" "$RO" "$DB"
run_test_match "qo_dolt_add_refused" "$QO SELECT dolt_add('t');" "$RO" "$DB"
run_test_match "qo_dolt_add_all_refused" "$QO SELECT dolt_add('-A');" "$RO" "$DB"
run_test_match "qo_dolt_commit_refused" "$QO SELECT dolt_commit('-am','under query_only');" "$RO" "$DB"
run_test_match "qo_dolt_commit_empty_refused" "$QO SELECT dolt_commit('--allow-empty','-m','empty');" "$RO" "$DB"
run_test_match "qo_dolt_merge_refused" "$QO SELECT dolt_merge('b');" "$RO" "$DB"
run_test_match "qo_dolt_tag_refused" "$QO SELECT dolt_tag('v1');" "$RO" "$DB"
run_test_match "qo_dolt_branch_create_refused" "$QO SELECT dolt_branch('nb');" "$RO" "$DB"
run_test_match "qo_dolt_branch_delete_refused" "$QO SELECT dolt_branch('-D','b');" "$RO" "$DB"
run_test_match "qo_dolt_checkout_b_refused" "$QO SELECT dolt_checkout('-b','nb2');" "$RO" "$DB"
run_test_match "qo_dolt_reset_hard_refused" "$QO SELECT dolt_reset('--hard');" "$RO" "$DB"
run_test_match "qo_dolt_remote_add_refused" "$QO SELECT dolt_remote('add','o','file:///nonexistent');" "$RO" "$DB"
run_test_match "qo_dolt_gc_refused" "$QO SELECT dolt_gc();" "$RO" "$DB"

run_test "qo_reads_ok" "$QO
SELECT count(*) FROM t;
SELECT count(*) FROM dolt_log;
SELECT table_name, staged, status FROM dolt_status;
SELECT group_concat(name) FROM dolt_branches;
" "2
2
t|0|modified
main,b" "$DB"

run_test "qo_state_unchanged" "
SELECT count(*) FROM t;
SELECT count(*) FROM dolt_log;
SELECT count(*) FROM dolt_tags;
SELECT group_concat(name) FROM dolt_branches;
SELECT count(*) FROM dolt_remotes;
SELECT table_name, staged, status FROM dolt_status;
" "2
2
0
main,b
0
t|0|modified" "$DB"

run_test "qo_toggle_off_allows_commit" "
PRAGMA query_only=1;
PRAGMA query_only=0;
SELECT length(dolt_commit('-am','after toggle'));
SELECT count(*) FROM dolt_status;
" "40
0" "$DB"

IMM="file:$DB?immutable=1"
run_test "immutable_select_ok" "SELECT count(*) FROM t; SELECT count(*) FROM dolt_log;" "2
3" "$IMM"
run_test_match "immutable_insert_refused" "INSERT INTO t VALUES(9,'x');" "$RO" "$IMM"
run_test_match "immutable_dolt_commit_refused" "SELECT dolt_commit('--allow-empty','-m','via immutable');" "$RO" "$IMM"
run_test_match "immutable_dolt_branch_refused" "SELECT dolt_branch('imm');" "$RO" "$IMM"
run_test "immutable_state_unchanged" "SELECT count(*) FROM t; SELECT count(*) FROM dolt_log; SELECT group_concat(name) FROM dolt_branches;" "2
3
main,b" "$DB"

dltest_finish
