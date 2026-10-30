#!/bin/bash
set -u
set -o pipefail
DOLTLITE="${1:-build/doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0; FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"
SEED="CREATE TABLE t(id INT PRIMARY KEY,v TEXT);
INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-Am','base');
UPDATE t SET v='one'; SELECT dolt_commit('-am','c1'); SELECT dolt_tag('c1');
INSERT INTO t VALUES(2,'two'); SELECT dolt_commit('-am','c2'); SELECT dolt_tag('c2');
INSERT INTO t VALUES(3,'three'); SELECT dolt_commit('-am','c3'); SELECT dolt_tag('c3');"
series_oracle() {
  local name="$1" setup="$2" dir="$TMPROOT/$1"
  mkdir -p "$dir/dl" "$dir/dt"
  local dl_query="SELECT 'R|row|' || id || '|' || v FROM t ORDER BY id;
SELECT 'R|log|' || message FROM dolt_log WHERE message LIKE 'Revert%';"
  local dt_query="SELECT concat('R|row|',id,'|',v) FROM t ORDER BY id;
SELECT concat('R|log|',message) FROM dolt_log WHERE message LIKE 'Revert%';"
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup
$dl_query" || true
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$(vc_oracle_translate_for_dolt "$setup")
$dt_query" -r csv || true
  if [ -s "$dir/dt.err" ]; then
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "FAIL: $name Dolt SQL error"; cat "$dir/dt.err"; return
  fi
  local dl_out dt_out
  dl_out=$(tr -d '\r"' < "$dir/dl.out" | grep '^R|' | sort || true)
  dt_out=$(tr -d '\r"' < "$dir/dt.out" | grep '^R|' | sort || true)
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}
for args in "'c3','c2'" "'HEAD','HEAD~1'" "'c2','c3'" "'c3','c2','c1'"; do
  case_id=$(( ${case_id:-0}+1 ))
  series_oracle "revert_series_$case_id" "$SEED
SELECT dolt_revert($args);"
done
series_oracle revert_series_merge "$SEED
SELECT dolt_checkout('-b','source'); INSERT INTO t VALUES(4,'source');
SELECT dolt_commit('-am','source'); SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'main'); SELECT dolt_commit('-am','main');
SELECT dolt_merge('source','-m','merged');
SELECT dolt_revert('HEAD','HEAD~1');"
series_oracle revert_series_schema "$SEED
ALTER TABLE t ADD COLUMN extra INT DEFAULT 9;
SELECT dolt_commit('-am','column'); SELECT dolt_tag('column');
CREATE INDEX ix ON t(extra); SELECT dolt_commit('-am','index'); SELECT dolt_tag('index');
INSERT INTO t VALUES(4,'four',10); SELECT dolt_commit('-am','row'); SELECT dolt_tag('row');
SELECT dolt_revert('row','index','column');"
for boundary in begin savepoint; do
  series_oracle "revert_series_txn_$boundary" "$SEED
$([ "$boundary" = begin ] && echo BEGIN || echo 'SAVEPOINT caller');
SELECT dolt_revert('c3','c2');"
done
for range in 'HEAD~2..HEAD' 'HEAD~2...HEAD'; do
  vc_oracle_error "revert_series_range_$range" "$SEED
SELECT dolt_revert('$range');"
done
vc_oracle_error revert_series_invalid_later "$SEED
SELECT dolt_revert('c3','absent');"
for finish in abort continue; do
  name="revert_series_$finish"
  setup="$SEED
UPDATE t SET v='latest' WHERE id=1; SELECT dolt_commit('-am','latest');
BEGIN;"
  dir="$TMPROOT/$name"; mkdir -p "$dir/dl" "$dir/dt"
  after="SELECT dolt_revert('--abort');"
  if [ "$finish" = continue ]; then
    after="SELECT dolt_conflicts_resolve('--theirs','t'); SELECT dolt_add('t'); SELECT dolt_revert('--continue');"
  fi
  dl_script="$setup
SELECT dolt_revert('c3','c1','c2');
SELECT 'R|pending|' || (SELECT count(*) FROM dolt_log WHERE message LIKE 'Revert%') || '|' || (SELECT count(*) FROM dolt_conflicts);
$after
SELECT 'R|row|' || id || '|' || v FROM t ORDER BY id;
SELECT 'R|log|' || message FROM dolt_log WHERE message LIKE 'Revert%';"
  dt_script="$(vc_oracle_translate_for_dolt "$setup")
CALL dolt_revert('c3','c1','c2');
SELECT concat('R|pending|',(SELECT count(*) FROM dolt_log WHERE message LIKE 'Revert%'),'|',(SELECT count(*) FROM dolt_conflicts));
$(vc_oracle_translate_for_dolt "$after")
SELECT concat('R|row|',id,'|',v) FROM t ORDER BY id;
SELECT concat('R|log|',message) FROM dolt_log WHERE message LIKE 'Revert%';"
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$dl_script" --expect-error || true
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dt_script" -r csv || true
  if [ -s "$dir/dt.err" ]; then
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "FAIL: $name Dolt SQL error"; cat "$dir/dt.err"; continue
  fi
  dl_out=$(tr -d '\r"' < "$dir/dl.out" | grep '^R|' | sort || true)
  dt_out=$(tr -d '\r"' < "$dir/dt.out" | grep '^R|' | sort || true)
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
done
vc_oracle_finish
