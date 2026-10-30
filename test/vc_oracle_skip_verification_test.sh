#!/bin/bash
set -u
set -o pipefail
DOLTLITE="${1:-build/doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0; FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

oracle_skip() {
  local name="$1" setup="$2" dir="$TMPROOT/$1"
  mkdir -p "$dir/dl" "$dir/dt"
  local query_dl="SELECT 'R|' || count(*) || '|' || (SELECT status FROM dolt_test_run('bad')) || '|' || (SELECT message FROM dolt_log LIMIT 1) || '|' || coalesce((SELECT staged FROM dolt_status WHERE table_name='t'),-1) FROM t;"
  local query_dt="SELECT concat('R|',count(*),'|',(SELECT status FROM dolt_test_run('bad')),'|',(SELECT message FROM dolt_log LIMIT 1),'|',coalesce((SELECT staged FROM dolt_status WHERE table_name='t'),-1)) FROM t;"
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup
$query_dl" || true
  local dolt_setup dl_out dt_out
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup
$query_dt" -r csv || true
  dl_out=$(tr -d '\r"' < "$dir/dl.out" | grep '^R|' || true)
  dt_out=$(tr -d '\r"' < "$dir/dt.out" | grep '^R|' || true)
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

SEED="CREATE TABLE t(id INT PRIMARY KEY);
INSERT INTO t VALUES(1);
INSERT INTO dolt_tests VALUES('bad','checks','SELECT 0','expected_single_value','==','1');
SELECT dolt_commit('--skip-verification','-Am','base');"
for args in "'--skip-verification','-am','next'" "'-Am','next','--skip-verification'"; do
  commit_case=$(( ${commit_case:-0} + 1 ))
  oracle_skip "commit_$commit_case" "$SEED
INSERT INTO t VALUES(2);
SELECT dolt_commit($args);"
done
for graph in ff diverged; do
  for mode in default noff nocommit squash; do
    setup="$SEED
SELECT dolt_checkout('-b','source');
INSERT INTO t VALUES(2);
SELECT dolt_commit('--skip-verification','-am','source');
SELECT dolt_checkout('main');"
    if [ "$graph" = diverged ]; then setup="$setup
INSERT INTO t VALUES(3);
SELECT dolt_commit('--skip-verification','-am','main');"; fi
    args="'--skip-verification','source','-m','merged'"
    if [ "$mode" = noff ]; then args="$args,'--no-ff'"; fi
    if [ "$mode" = nocommit ]; then args="$args,'--no-commit'"; fi
    if [ "$mode" = squash ]; then args="$args,'--squash'"; fi
    oracle_skip "merge_${graph}_$mode" "$setup
SELECT dolt_merge($args);"
  done
done
vc_oracle_error "merge_skip_abort_extra" "
SELECT dolt_merge('--abort','--skip-verification');
"
vc_oracle_error "merge_skip_conflicting_modes" "
CREATE TABLE t(id INT PRIMARY KEY);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('source');
SELECT dolt_merge('--skip-verification','--squash','--no-ff','source');
"
vc_oracle_finish
