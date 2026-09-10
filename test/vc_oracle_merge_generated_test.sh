#!/usr/bin/env bash
set -euo pipefail

DOLTLITE="${1:?usage: $0 doltlite dolt}"
DOLT="${2:-dolt}"
DOLTLITE="$(cd "$(dirname "$DOLTLITE")" && pwd)/$(basename "$DOLTLITE")"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0
fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

for indexed in 0 1; do
  for right in 2 3; do
    name="generated_${indexed}_${right}"
    dir="$TMPROOT/$name"
    mkdir -p "$dir/dolt"
    index_sql=""
    if [ "$indexed" = 1 ]; then index_sql='CREATE INDEX iz ON t(z);'; fi
    setup="
CREATE TABLE t(id INTEGER PRIMARY KEY,x INT,z INT AS(x+y) STORED,
               y INT,w INT AS((x+y)*2) STORED,v INT AS((x+y)*2+1) VIRTUAL);
$index_sql
INSERT INTO t(id,x,y) VALUES(1,1,1);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feature');
UPDATE t SET x=2;
SELECT dolt_commit('-Am','left');
SELECT dolt_checkout('feature');
UPDATE t SET y=$right;
SELECT dolt_commit('-Am','right');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');"
    if ! vc_oracle_run_doltlite_script "$dir/db" "$dir/dl.out" "$dir/dl.err" "$setup"; then
      fail=$((fail+1))
      echo "FAIL: $name: DoltLite setup/merge"
      cat "$dir/dl.err"
      continue
    fi
    dt_setup=$(vc_oracle_translate_for_dolt "$setup")
    if ! vc_oracle_run_dolt_script "$dir/dolt" "$dir/dt.out" "$dir/dt.err" "$dt_setup"; then
      fail=$((fail+1))
      echo "FAIL: $name: Dolt setup/merge"
      cat "$dir/dt.err"
      continue
    fi
    query="SELECT concat('R|',x,'|',y,'|',z,'|',w,'|',v) FROM t WHERE id=1;"
    dl_out=$("$DOLTLITE" "$dir/db" "$query")
    dt_out=$(cd "$dir/dolt" && "$DOLT" sql -r csv -q "$query")
    dt_out=$(printf '%s\n' "$dt_out" | tail -n +2)
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out" || true
    expected="R|2|$right|$((2+right))|$(((2+right)*2))|$(((2+right)*2+1))"
    vc_oracle_assert_match "${name}_expected" "$dl_out" "$expected" || true
  done
done

echo "Results: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
