#!/bin/bash
set -u
set -o pipefail
DOLTLITE="${1:-build/doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0; FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"
SEED="CREATE TABLE t(id INT PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','target');
INSERT INTO t VALUES(2); SELECT dolt_add('t'); INSERT INTO t VALUES(3);
CREATE TABLE added(id INT PRIMARY KEY); INSERT INTO added VALUES(9);
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4); SELECT dolt_add('t'); INSERT INTO t VALUES(5);"
for mode in staged modified all amend allow skip; do
  for boundary in auto begin; do
    name="commit_branch_${mode}_$boundary"
    dir="$TMPROOT/$name"; mkdir -p "$dir/dl" "$dir/dt"
    setup="$SEED"
    args="'--branch','target','-m','target commit','--author','Writer <writer@test>'"
    case "$mode" in
      modified) args="$args,'-a'" ;;
      all) args="$args,'-A'" ;;
      amend) args="$args,'--amend'" ;;
      allow|skip)
        setup="$setup
SELECT dolt_checkout('target'); SELECT dolt_reset('--hard'); SELECT dolt_checkout('main');"
        if [ "$mode" = allow ]; then args="$args,'--allow-empty'"; else args="$args,'--skip-empty'"; fi ;;
    esac
    if [ "$boundary" = begin ]; then setup="$setup
BEGIN; SELECT id FROM t;"; fi
    setup="$setup
SELECT dolt_commit($args);"
    dl_query="SELECT 'R|caller|' || active_branch() || '|' || group_concat(id,',') || '|' || (SELECT count(*) FROM dolt_status WHERE staged) FROM (SELECT id FROM t ORDER BY id);
SELECT 'R|target|' || group_concat(id,',') FROM (SELECT id FROM dolt_at_t('target') ORDER BY id);
SELECT 'R|log|' || message FROM dolt_log('target') LIMIT 1;
SELECT dolt_checkout('target');
SELECT 'R|working|' || group_concat(id,',') FROM (SELECT id FROM t ORDER BY id);"
    dt_query="SELECT concat('R|caller|',active_branch(),'|',group_concat(id ORDER BY id),'|',(SELECT count(*) FROM dolt_status WHERE staged)) FROM t;
SELECT concat('R|target|',group_concat(id ORDER BY id)) FROM t AS OF 'target';
SELECT concat('R|log|',message) FROM dolt_log('target') LIMIT 1;
CALL dolt_checkout('target');
SELECT concat('R|working|',group_concat(id ORDER BY id)) FROM t;"
    vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup
$dl_query" || true
    dt_setup=$(vc_oracle_translate_for_dolt "$setup")
    vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dt_setup
$dt_query" -r csv || true
    if [ -s "$dir/dt.err" ]; then
      fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
      echo "FAIL: $name Dolt SQL error"; cat "$dir/dt.err"; continue
    fi
    dl_out=$(tr -d '\r"' < "$dir/dl.out" | grep '^R|' || true)
    dt_out=$(tr -d '\r"' < "$dir/dt.out" | grep '^R|' || true)
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  done
done
for args in "'--branch','missing','-am','no'" "'--branch','target','-m','no','--date','bad'" "'--branch','target','-m','no','--author','bad'"; do
  vc_oracle_error "commit_branch_invalid_${error_case:-0}" "$SEED
SELECT dolt_commit($args);"
  error_case=$(( ${error_case:-0}+1 ))
done
vc_oracle_finish
