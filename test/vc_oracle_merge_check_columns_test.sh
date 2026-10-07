#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0; FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

oracle() {
  local name="$1" column="$2" expression="$3" invalid="$4" direction="$5"
  local dir="$TMPROOT/$name" setup dl_rc=0 dt_rc=0 dl_out dt_out
  local check_branch=feat drop_branch=main quoted_column
  if [ "$direction" = reverse ]; then check_branch=main; drop_branch=feat; fi
  quoted_column='`'"$column"'`'
  mkdir -p "$dir/dl" "$dir/dt"
  setup="CREATE TABLE t(id INTEGER PRIMARY KEY,v INT,w INT,$quoted_column INT);
INSERT INTO t VALUES(1,1,1,1);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('$check_branch');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY,v INT,w INT,$quoted_column INT,
               CONSTRAINT ck CHECK($expression));
INSERT INTO t VALUES(1,1,1,1);
SELECT dolt_commit('-Am','check');
SELECT dolt_checkout('$drop_branch');
ALTER TABLE t DROP COLUMN $quoted_column;
SELECT dolt_commit('-Am','drop');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');"
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" \
    "$setup" || dl_rc=$?
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" \
    "$(vc_oracle_translate_for_dolt "$setup")" || dt_rc=$?
  if [ "$dl_rc" -ne 0 ] || [ "$dt_rc" -ne 0 ]; then
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES ${name}_merge"
    echo "  FAIL: ${name}_merge (doltlite=$dl_rc dolt=$dt_rc)"
    cat "$dir/dl.err" "$dir/dt.err"
    return
  fi
  pass=$((pass+1))
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.query" "$dir/dl.err" \
    "SELECT 'Q|'||(SELECT count(*) FROM pragma_table_info('t'))||'|'||
             (SELECT count(*) FROM sqlite_master WHERE name='t' AND sql LIKE '%CHECK(%')||
             '|'||id||'|'||v||'|'||w FROM t;" || dl_rc=$?
  (cd "$dir/dt" && "$DOLT" sql -r csv -q \
    "SELECT CONCAT('Q|',(SELECT count(*) FROM information_schema.columns WHERE table_name='t'),
                   '|',(SELECT count(*) FROM information_schema.check_constraints),
                   '|',id,'|',v,'|',w) FROM t;") >"$dir/dt.query" 2>"$dir/dt.err" || dt_rc=$?
  dl_out=$(tr -d '\r"' <"$dir/dl.query" | grep '^Q|')
  dt_out=$(tr -d '\r"' <"$dir/dt.query" | grep '^Q|')
  if [ "$dl_rc" -eq 0 ] && [ "$dt_rc" -eq 0 ] \
     && [ "$dl_out" = 'Q|3|1|1|1|1' ] && [ "$dt_out" = "$dl_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES ${name}_schema"
    echo "  FAIL: ${name}_schema (doltlite=$dl_out dolt=$dt_out)"
  fi
  dl_rc=0; dt_rc=0
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" \
    "INSERT INTO t VALUES(2,$invalid,-1);" --expect-error || dl_rc=$?
  (cd "$dir/dt" && "$DOLT" sql -q "INSERT INTO t VALUES(2,$invalid,-1);") \
    >"$dir/dt.out" 2>"$dir/dt.err" || dt_rc=$?
  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc" \
     && grep -qi 'check.*constraint' "$dir/dl.err" \
     && grep -qi 'check.*constraint' "$dir/dt.err"; then
    pass=$((pass+1))
  else
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES ${name}_enforcement"
    echo "  FAIL: ${name}_enforcement (doltlite=$dl_rc dolt=$dt_rc)"
  fi
}

vc_oracle_require_working_engines "$TMPROOT/probe"
while IFS='|' read -r name column expression invalid; do
  for direction in forward reverse; do
    oracle "${name}_${direction}" "$column" "$expression" "$invalid" "$direction"
  done
done <<'CASES'
end|end|CASE WHEN v>0 THEN w ELSE 0 END>0|-1
case|case|CASE WHEN v>0 THEN w ELSE 0 END>0|-1
when|when|CASE WHEN v>0 THEN w ELSE 0 END>0|-1
then|then|CASE WHEN v>0 THEN w ELSE 0 END>0|-1
else|else|CASE WHEN v>0 THEN w ELSE 0 END>0|-1
not|not|v IS NOT NULL|NULL
is|is|v IS NOT NULL|NULL
null|null|v IS NOT NULL|NULL
and|and|v>0 AND w>0|-1
or|or|v>0 OR w>0|-1
between|between|v BETWEEN 1 AND 3|-1
like|like|v LIKE '1%'|-1
function|abs|abs(v)>0|0
literal|end|v>0 AND length('END NOT')>0|-1
CASES

for column in end not; do
  for direction in forward reverse; do
    check_branch=feat; drop_branch=main
    if [ "$direction" = reverse ]; then check_branch=main; drop_branch=feat; fi
    quoted_column='`'"$column"'`'
    vc_oracle_error "actual_${column}_${direction}" "
CREATE TABLE t(id INTEGER PRIMARY KEY,v INT,$quoted_column INT);
INSERT INTO t VALUES(1,1,1);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('$check_branch');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY,v INT,$quoted_column INT,
               CONSTRAINT ck CHECK($quoted_column>0));
INSERT INTO t VALUES(1,1,1);
SELECT dolt_commit('-Am','check');
SELECT dolt_checkout('$drop_branch');
ALTER TABLE t DROP COLUMN $quoted_column;
SELECT dolt_commit('-Am','drop');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');"
  done
done

vc_oracle_finish
