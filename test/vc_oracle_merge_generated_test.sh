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

# Rows that reach the merge only from their side: an insert, and a modify whose
# base and ours both hold NULL. Neither goes through cell merge, so the stored
# column has to be recomputed from the merged layout on the way in.
for indexed in 0 1; do
  name="onesided_${indexed}"
  dir="$TMPROOT/$name"
  mkdir -p "$dir/dolt"
  index_sql=""
  if [ "$indexed" = 1 ]; then index_sql='CREATE INDEX iz ON t(z);'; fi
  # SQLite refuses ALTER TABLE ADD COLUMN for a STORED column, so DoltLite
  # reaches the same schema through a table rebuild. Dolt sees a rebuild as a
  # drop plus a create, which is a different merge, hence per-dialect setup.
  head="
CREATE TABLE t(id INT PRIMARY KEY, x INT, y INT);
INSERT INTO t(id,x,y) VALUES(1,1,1),(2,2,NULL);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feature');"
  tail_sql="
$index_sql
SELECT dolt_commit('-Am','left adds stored column');
SELECT dolt_checkout('feature');
INSERT INTO t(id,x,y) VALUES(3,7,8);
UPDATE t SET y=5 WHERE id=2;
SELECT dolt_commit('-Am','right inserts and updates');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');"
  setup="$head
CREATE TABLE t2(id INT PRIMARY KEY, x INT, y INT, z INT AS(x+y) STORED);
INSERT INTO t2(id,x,y) SELECT id,x,y FROM t;
DROP TABLE t;
ALTER TABLE t2 RENAME TO t;
$tail_sql"
  if ! vc_oracle_run_doltlite_script "$dir/db" "$dir/dl.out" "$dir/dl.err" "$setup"; then
    fail=$((fail+1))
    echo "FAIL: $name: DoltLite setup/merge"
    cat "$dir/dl.err"
    continue
  fi
  dt_setup=$(vc_oracle_translate_for_dolt "$head
ALTER TABLE t ADD COLUMN z INT AS (x+y) STORED;
$tail_sql")
  if ! vc_oracle_run_dolt_script "$dir/dolt" "$dir/dt.out" "$dir/dt.err" "$dt_setup"; then
    fail=$((fail+1))
    echo "FAIL: $name: Dolt setup/merge"
    cat "$dir/dt.err"
    continue
  fi
  query="SELECT concat('R|',id,'|',x,'|',y,'|',z) FROM t ORDER BY id;"
  dl_out=$("$DOLTLITE" "$dir/db" "$query")
  dt_out=$(cd "$dir/dolt" && "$DOLT" sql -r csv -q "$query")
  dt_out=$(printf '%s\n' "$dt_out" | tail -n +2)
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out" || true
  expected="R|1|1|1|2
R|2|2|5|7
R|3|7|8|15"
  vc_oracle_assert_match "${name}_expected" "$dl_out" "$expected" || true
done

echo "Results: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
