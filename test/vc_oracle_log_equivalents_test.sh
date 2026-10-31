#!/bin/bash
set -u
set -o pipefail
DOLTLITE="${1:-build/doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0; FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"
ROOT=$(cd "$(dirname "$0")/.." && pwd)

log_equivalent() {
  local name="$1" setup="$2" dl_query="$3" dt_query="$4" allow_empty="${5:-}"
  local dir="$TMPROOT/$name" dl_out dt_out dolt_setup
  mkdir -p "$dir/dl" "$dir/dt"
  if ! vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup
$dl_query"; then
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "FAIL: $name DoltLite execution"; cat "$dir/dl.err"; return
  fi
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  if ! vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup
$dt_query" -r csv; then
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "FAIL: $name Dolt execution"; cat "$dir/dt.err"; return
  fi
  if [ -s "$dir/dt.err" ]; then
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "FAIL: $name Dolt SQL error"; cat "$dir/dt.err"; return
  fi
  dl_out=$(tr -d '\r"' < "$dir/dl.out" | grep '^R|' | sort || true)
  dt_out=$(tr -d '\r"' < "$dir/dt.out" | grep '^R|' | sort || true)
  if [ "$allow_empty" = empty ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

DL_ROW="CASE WHEN (SELECT count(parent_hash) FROM dolt_commit_ancestors a WHERE a.commit_hash=l.commit_hash)=0 THEN 'ROOT' ELSE l.message END"
DT_ROW="CASE WHEN (SELECT count(parent_hash) FROM dolt_commit_ancestors a WHERE a.commit_hash=l.commit_hash)=0 THEN 'ROOT' ELSE l.message END"
SEED="CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(100));
CREATE TABLE other(id INT PRIMARY KEY);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('source');
INSERT INTO other VALUES(1);
SELECT dolt_commit('-am','main other');
SELECT dolt_checkout('source');
INSERT INTO t VALUES(2,'source');
SELECT dolt_commit('-am','source data');
SELECT dolt_checkout('main');
SELECT dolt_merge('source','-m','merge source');"
for minimum in 0 1 2 3; do
  log_equivalent "min_parents_$minimum" "$SEED" \
    "SELECT 'R|' || $DL_ROW FROM dolt_log l WHERE (SELECT count(parent_hash) FROM dolt_commit_ancestors a WHERE a.commit_hash=l.commit_hash)>=$minimum;" \
    "SELECT concat('R|',$DT_ROW) FROM dolt_log('--min-parents','$minimum') l;" \
    "$([ "$minimum" = 3 ] && echo empty)"
done
log_equivalent merges "$SEED" \
  "SELECT 'R|' || $DL_ROW FROM dolt_log l WHERE (SELECT count(parent_hash) FROM dolt_commit_ancestors a WHERE a.commit_hash=l.commit_hash)>1;" \
  "SELECT concat('R|',$DT_ROW) FROM dolt_log('--merges') l;"
log_equivalent excluded_revision "$SEED" \
  "SELECT 'R|' || $DL_ROW FROM dolt_log('source..main') l;" \
  "SELECT concat('R|',$DT_ROW) FROM dolt_log('main','--not','source') l;"
log_equivalent two_revisions "$SEED" \
  "SELECT 'R|' || $DL_ROW FROM (SELECT * FROM dolt_log('main') UNION SELECT * FROM dolt_log('source')) l;" \
  "SELECT concat('R|',$DT_ROW) FROM dolt_log('main','source') l;"

TABLE_QUERY=$(python3 - "$ROOT/doc/doltlite/log_table_filter.sql" <<'PYQUERY'
import sys
s=open(sys.argv[1]).read().replace(':revision',"'HEAD'").replace(':table_name',"'t'")
print(s.rstrip().rstrip(';'))
PYQUERY
)
for scenario in merged linear schema empty dropped renamed first_parent same_missing_parent drop_parent merge_create root; do
  setup="$SEED"
  case "$scenario" in
    linear) setup="$SEED
INSERT INTO t VALUES(3,'linear'); SELECT dolt_commit('-am','linear data');" ;;
    schema) setup="$SEED
CREATE INDEX ix_v ON t(v); SELECT dolt_commit('-Am','index change');
ALTER TABLE t ADD COLUMN extra INT; SELECT dolt_commit('-Am','column change');" ;;
    empty) setup="CREATE TABLE t(id INT PRIMARY KEY); SELECT dolt_commit('-Am','empty create');" ;;
    dropped) setup="$SEED
DROP TABLE t; SELECT dolt_commit('-am','drop t');" ;;
    renamed) setup="$SEED
ALTER TABLE t RENAME TO renamed; SELECT dolt_commit('-Am','rename t');" ;;
    first_parent) setup="CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(100));
CREATE TABLE other(id INT PRIMARY KEY); INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('source');
INSERT INTO t VALUES(2,'main'); SELECT dolt_commit('-am','main data');
SELECT dolt_checkout('source'); INSERT INTO other VALUES(1);
SELECT dolt_commit('-am','source other'); SELECT dolt_checkout('main');
SELECT dolt_merge('source','-m','merge unchanged first parent');" ;;
    same_missing_parent) setup="CREATE TABLE other(id INT PRIMARY KEY);
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('source');
CREATE TABLE t(id INT PRIMARY KEY); SELECT dolt_commit('-Am','main creates t');
SELECT dolt_checkout('source'); INSERT INTO other VALUES(1);
SELECT dolt_commit('-am','source other'); SELECT dolt_checkout('main');
SELECT dolt_merge('source','-m','merge unchanged one parent');" ;;
    drop_parent) setup="$SEED
SELECT dolt_branch('dropper'); INSERT INTO other VALUES(2);
SELECT dolt_commit('-am','main other again'); SELECT dolt_checkout('dropper');
DROP TABLE t; SELECT dolt_commit('-am','drop on source');
SELECT dolt_checkout('main'); SELECT dolt_merge('dropper','-m','merge drop');" ;;
    merge_create) setup="CREATE TABLE other(id INT PRIMARY KEY);
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('source');
INSERT INTO other VALUES(1); SELECT dolt_commit('-am','main other');
SELECT dolt_checkout('source'); INSERT INTO other VALUES(2);
SELECT dolt_commit('-am','source other'); SELECT dolt_checkout('main');
SELECT dolt_merge('source','--no-commit'); CREATE TABLE t(id INT PRIMARY KEY);
SELECT dolt_commit('-Am','merge creates t');" ;;
    root) setup="CREATE TABLE t(id INT PRIMARY KEY); SELECT dolt_commit('--amend','-Am','amended root');" ;;
  esac
  log_equivalent "table_$scenario" "$setup" \
    "SELECT 'R|' || $DL_ROW FROM ($TABLE_QUERY) l;" \
    "SELECT concat('R|',$DT_ROW) FROM dolt_log('HEAD','--tables','t') l;" \
    "$([ "$scenario" = root ] && echo empty)"
done
TABLE_SOURCE_QUERY=$(printf '%s' "$TABLE_QUERY" | sed "s/'HEAD'/'source'/g")
TABLE_RANGE_QUERY=$(printf '%s' "$TABLE_QUERY" | sed "s/'HEAD'/'source..main'/g")
log_equivalent table_noncurrent "$SEED
INSERT INTO t VALUES(3,'main only'); SELECT dolt_commit('-am','main only data');" \
  "SELECT 'R|' || $DL_ROW FROM ($TABLE_SOURCE_QUERY) l;" \
  "SELECT concat('R|',$DT_ROW) FROM dolt_log('source','--tables','t') l;"
log_equivalent table_range "$SEED" \
  "SELECT 'R|' || $DL_ROW FROM ($TABLE_RANGE_QUERY) l;" \
  "SELECT concat('R|',$DT_ROW) FROM dolt_log('main','--not','source','--tables','t') l;"
vc_oracle_finish
