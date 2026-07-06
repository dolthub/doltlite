#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

DB="$TMPDIR/gc-concurrent.db"
WORKERS="${DOLTLITE_GC_STRESS_WORKERS:-4}"
OPS="${DOLTLITE_GC_STRESS_OPS:-40}"
GC_OPS="${DOLTLITE_GC_STRESS_GC_OPS:-30}"
FAILED=0

run_sql() {
  "$DOLTLITE" "$DB" "$1"
}

run_sql "
PRAGMA busy_timeout=10000;
CREATE TABLE t(id INTEGER PRIMARY KEY, worker INTEGER, step INTEGER, payload BLOB);
INSERT INTO t VALUES(0, 0, 0, randomblob(16384));
" >/dev/null || exit 1

writer() {
  local w="$1"
  local i id
  for i in $(seq 1 "$OPS"); do
    id=$((w*100000+i))
    if ! DOLTLITE_CHUNK_PENDING_DRAIN_LIMIT=4096 "$DOLTLITE" "$DB" "
      PRAGMA busy_timeout=10000;
      INSERT OR REPLACE INTO t VALUES($id, $w, $i, randomblob(16384));
    " >/dev/null 2>"$TMPDIR/writer-$w-$i.err"; then
      cat "$TMPDIR/writer-$w-$i.err"
      exit 1
    fi
  done
}

gc_loop() {
  local i out rc
  for i in $(seq 1 "$GC_OPS"); do
    out=$(DOLTLITE_CHUNK_PENDING_DRAIN_LIMIT=4096 "$DOLTLITE" "$DB" "
      PRAGMA busy_timeout=10000;
      SELECT dolt_gc();
    " 2>&1)
    rc=$?
    if [ "$rc" -ne 0 ]; then
      if echo "$out" | grep -qE 'database is locked|gc requires exclusive access|failed to acquire lock for gc'; then
        sleep 0.05
        continue
      fi
      echo "$out"
      exit 1
    fi
    sleep 0.02
  done
}

pids=""
gc_loop &
pids="$pids $!"
for w in $(seq 1 "$WORKERS"); do
  writer "$w" &
  pids="$pids $!"
done

for pid in $pids; do
  if ! wait "$pid"; then
    FAILED=1
  fi
done

if [ "$FAILED" -ne 0 ]; then
  exit 1
fi

actual=$(run_sql "SELECT count(*) FROM t;" 2>&1) || {
  echo "$actual"
  exit 1
}
expected=$((WORKERS*OPS+1))
if [ "$actual" != "$expected" ]; then
  echo "wrong final row count: expected $expected got $actual"
  exit 1
fi

integrity=$(run_sql "PRAGMA integrity_check;" 2>&1) || {
  echo "$integrity"
  exit 1
}
if [ "$integrity" != "ok" ]; then
  echo "$integrity"
  exit 1
fi

gc=$(run_sql "SELECT dolt_gc();" 2>&1) || {
  echo "$gc"
  exit 1
}
if echo "$gc" | grep -q 'gc mark phase failed'; then
  echo "$gc"
  exit 1
fi

echo "PASS gc concurrent commit stress"
