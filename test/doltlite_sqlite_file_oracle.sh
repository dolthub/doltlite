#!/bin/bash

DOLTLITE="${1:-./doltlite}"
SQLITE3=$(command -v sqlite3 2>/dev/null || echo /usr/bin/sqlite3)
TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT
PASS=0
FAIL=0
SKIP=0
ERRORS=""

want_eq() {
  local name="$1" got="$2" want="$3"
  got="${got//$'\r'/}"
  want="${want//$'\r'/}"
  if [ "$got" = "$want" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\n  FAIL: $name\n    want: $(printf %q "$want")\n    got:  $(printf %q "$got")"
  fi
}

oracle() {
  local name="$1" sql="$2" db="$3"
  local s d
  s=$(printf '%s\n' "$sql" | $SQLITE3 "$db" 2>&1)
  d=$(printf '%s\n' "$sql" | $DOLTLITE "$db" 2>&1)
  s="${s//$'\r'/}"
  d="${d//$'\r'/}"
  if [ "$s" = "$d" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\n  FAIL: $name\n    sqlite3:  $(printf %q "$s")\n    doltlite: $(printf %q "$d")"
  fi
}

seed_stock() {
  rm -f "$1" "$1-wal" "$1-shm" "$1-journal"
  $SQLITE3 "$1" "$2" >/dev/null
}

echo "=== doltlite vs stock sqlite3 on a sqlite3-created file ==="

if [ ! -x "$SQLITE3" ]; then
  echo "SKIP: stock sqlite3 binary not found at $SQLITE3"
  echo "Results: 0 passed, 0 failed, 1 skipped"
  exit 0
fi

echo ""
echo "--- INTEGER PK / page sizes ---"

for page_size in 512 1024 4096 8192 16384 65536; do
  DB=$TMP/intpk-$page_size.db
  seed_stock "$DB" "PRAGMA page_size=$page_size; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(5,'e'),(8,'h'),(10,'j');"
  want_eq "O_intpk_${page_size}_engine_is_orig" \
    "$(printf '%s\n' "SELECT doltlite_engine();" | $DOLTLITE "$DB" 2>&1 | tr -d '\r' | tail -1)" \
    "orig"
  oracle "O_intpk_${page_size}_page_size" "PRAGMA page_size;" "$DB"
  oracle "O_intpk_${page_size}_count" "SELECT count(*) FROM t;" "$DB"
  oracle "O_intpk_${page_size}_by_pk" "SELECT v FROM t WHERE id=8;" "$DB"
  oracle "O_intpk_${page_size}_between_last" "SELECT count(*) FROM t WHERE id BETWEEN 8 AND 10;" "$DB"
  oracle "O_intpk_${page_size}_between_gap" "SELECT count(*) FROM t WHERE id BETWEEN 6 AND 7;" "$DB"
  oracle "O_intpk_${page_size}_between_all" "SELECT count(*) FROM t WHERE id BETWEEN 1 AND 10;" "$DB"
done

oracle "O_intpk_between_last" "SELECT count(*) FROM t WHERE id BETWEEN 8 AND 10;" "$TMP/intpk-4096.db"

echo ""
echo "--- overflow DISTINCT ---"

for page_size in 1024 4096 8192 16384; do
  DB=$TMP/ov-$page_size.db
  seed_stock "$DB" "PRAGMA page_size=$page_size; CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB, s TEXT); WITH r(i) AS (VALUES(1),(2),(3)) INSERT INTO t SELECT i, CAST(char(64+i)||substr(hex(zeroblob(25000)),1,49999) AS BLOB), char(96+i)||substr(hex(zeroblob(25000)),1,49999) FROM r;"
  oracle "O_overflow_${page_size}_distinct" \
    "SELECT (SELECT count(DISTINCT b) FROM t)||':'||(SELECT count(DISTINCT s) FROM t);" "$DB"
  oracle "O_overflow_${page_size}_group" \
    "SELECT (SELECT count(*) FROM (SELECT b FROM t GROUP BY b))||':'||(SELECT count(*) FROM (SELECT s FROM t GROUP BY s));" "$DB"
done

echo ""
echo "--- WAL round-trip ---"

DB=$TMP/wal.db
seed_stock "$DB" "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');"
oracle "O_wal_mode" "PRAGMA journal_mode;" "$DB"
oracle "O_wal_count_seed" "SELECT count(*) FROM t;" "$DB"
printf '%s\n' "INSERT INTO t VALUES(2,'b');" | $DOLTLITE "$DB" >/dev/null
want_eq "O_wal_sqlite3_sees_doltlite_insert" \
  "$(printf '%s\n' "SELECT count(*)||group_concat(v,'') FROM (SELECT v FROM t ORDER BY id);" | $SQLITE3 "$DB" | tr -d '\r')" \
  "2ab"
printf '%s\n' "INSERT INTO t VALUES(3,'c');" | $SQLITE3 "$DB" >/dev/null
want_eq "O_wal_doltlite_sees_sqlite3_insert" \
  "$(printf '%s\n' "SELECT count(*)||group_concat(v,'') FROM (SELECT v FROM t ORDER BY id);" | $DOLTLITE "$DB" 2>&1 | tr -d '\r' | tail -1)" \
  "3abc"

echo ""
echo "============================="
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
echo "============================="
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
