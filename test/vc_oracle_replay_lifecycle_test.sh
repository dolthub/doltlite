#!/bin/bash
set -u
set -o pipefail
DOLTLITE="${1:-build/doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0; FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

replay_oracle() {
  local kind="$1" finish="$2" resolution="$3"
  local name="${kind}_${finish}_${resolution}" dir="$TMPROOT/${kind}_${finish}_${resolution}"
  local setup="CREATE TABLE t(id INT PRIMARY KEY,v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-Am','base');"
  mkdir -p "$dir/dl" "$dir/dt"
  if [ "$kind" = cherry_pick ]; then setup="$setup
SELECT dolt_checkout('-b','target');"; fi
  setup="$setup
UPDATE t SET v='target';
SELECT dolt_commit('-am','target edit');"
  if [ "$kind" = cherry_pick ]; then setup="$setup
SELECT dolt_checkout('main');"
  else setup="$setup
SELECT dolt_branch('target');"; fi
  setup="$setup
UPDATE t SET v='main';
SELECT dolt_commit('-am','main edit');"
  local tail="SELECT dolt_${kind}('--$finish');"
  if [ "$finish" = continue ]; then
    if [ "$resolution" = manual ]; then
      tail="UPDATE t SET v='manual'; DELETE FROM dolt_conflicts_t;"
    else
      tail="SELECT dolt_conflicts_resolve('--$resolution','t');"
    fi
    tail="$tail
SELECT dolt_add('t');
SELECT dolt_${kind}('--continue');"
  fi
  local script="$setup
BEGIN;
SELECT dolt_${kind}('target');
SELECT 'PENDING|' || (SELECT count(*) FROM dolt_conflicts);
$tail
SELECT 'AFTER|' || (SELECT v FROM t) || '|' ||
 (SELECT count(*) FROM dolt_conflicts) || '|' ||
 (SELECT count(*) FROM dolt_status) || '|' ||
 (SELECT message FROM dolt_log LIMIT 1) || '|' ||
 (SELECT count(*) FROM dolt_log);
COMMIT;"
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$script" --allow-error || true
  local dolt_script
  dolt_script=$(vc_oracle_translate_for_dolt "$setup")
  dolt_script="$dolt_script
START TRANSACTION;
CALL dolt_${kind}('target');
SELECT concat('PENDING|',(SELECT count(*) FROM dolt_conflicts));
$(vc_oracle_translate_for_dolt "$tail")
SELECT concat('AFTER|',(SELECT v FROM t),'|',
 (SELECT count(*) FROM dolt_conflicts),'|',
 (SELECT count(*) FROM dolt_status),'|',
 (SELECT message FROM dolt_log LIMIT 1),'|',
 (SELECT count(*) FROM dolt_log));
COMMIT;"
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_script" --continue -r csv || true
  local dl_out dt_out
  dl_out=$(tr -d '\r"' < "$dir/dl.out" | grep -E '^(PENDING|AFTER)\|' || true)
  dt_out=$(tr -d '\r"' < "$dir/dt.out" | grep -E '^(PENDING|AFTER)\|' || true)
  if ! grep -q '^PENDING|1$' <<< "$dl_out" || ! grep -q '^PENDING|1$' <<< "$dt_out"; then
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name did not establish a conflict on both engines"
    return
  fi
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

for kind in cherry_pick revert; do
  for finish in abort continue; do
    for resolution in ours theirs manual; do
      replay_oracle "$kind" "$finish" "$resolution"
    done
  done
  for lifecycle in abort continue; do
    vc_oracle_error "${kind}_${lifecycle}_idle" "
CREATE TABLE t(id INT PRIMARY KEY);
SELECT dolt_commit('-Am','base');
SELECT dolt_${kind}('--$lifecycle');
"
  done
  vc_oracle_error "${kind}_contradictory" "
SELECT dolt_${kind}('--abort','--continue');
"
done
vc_oracle_finish
