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
  local s d s_rc d_rc
  s=$(printf '%s\n' "$sql" | $SQLITE3 "$db" 2>&1)
  s_rc=$?
  d=$(printf '%s\n' "$sql" | $DOLTLITE "$db" 2>&1)
  d_rc=$?
  s="${s//$'\r'/}"
  d="${d//$'\r'/}"
  if [ "$s_rc" -eq 0 ] && [ "$d_rc" -eq 0 ] && [ "$s" = "$d" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\n  FAIL: $name\n    sqlite3 rc=$s_rc $(printf %q "$s")\n    doltlite rc=$d_rc $(printf %q "$d")"
  fi
}

seed_stock() {
  rm -f "$1" "$1-wal" "$1-shm" "$1-journal"
  if ! $SQLITE3 "$1" "$2" >/dev/null; then
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\n  FAIL: seed $1"
  fi
}

copy_db() {
  cp "$1" "$2"
  [ -f "$1-wal" ] && cp "$1-wal" "$2-wal" || true
  [ -f "$1-shm" ] && cp "$1-shm" "$2-shm" || true
}

# Mutating SQL: each engine gets its own copy. Then a fresh sqlite3 reads the
# doltlite-mutated file and a fresh doltlite reads the sqlite3-mutated file.
oracle_copy() {
  local name="$1" sql="$2" src="$3" reopen="${4:-}"
  local sdb="$TMP/ora-s.db" ddb="$TMP/ora-d.db"
  local s d s_rc d_rc
  rm -f "$sdb" "$sdb-wal" "$sdb-shm" "$ddb" "$ddb-wal" "$ddb-shm"
  copy_db "$src" "$sdb"
  copy_db "$src" "$ddb"
  s=$(printf '%s\n' "$sql" | $SQLITE3 "$sdb" 2>&1)
  s_rc=$?
  d=$(printf '%s\n' "$sql" | $DOLTLITE "$ddb" 2>&1)
  d_rc=$?
  s="${s//$'\r'/}"
  d="${d//$'\r'/}"
  if [ "$s_rc" -eq 0 ] && [ "$d_rc" -eq 0 ] && [ "$s" = "$d" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\n  FAIL: $name\n    sqlite3 rc=$s_rc $(printf %q "$s")\n    doltlite rc=$d_rc $(printf %q "$d")"
  fi
  if [ -n "$reopen" ]; then
    oracle "${name}_sqlite3_reads_dl" "$reopen" "$ddb"
    oracle "${name}_dl_reads_sqlite3" "$reopen" "$sdb"
  fi
}

both_contain() {
  local name="$1" sql="$2" db="$3" needle="$4"
  local s d
  s=$(printf '%s\n' "$sql" | $SQLITE3 "$db" 2>&1)
  d=$(printf '%s\n' "$sql" | $DOLTLITE "$db" 2>&1)
  s="${s//$'\r'/}"
  d="${d//$'\r'/}"
  if echo "$s" | grep -q -- "$needle" && echo "$d" | grep -q -- "$needle"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\n  FAIL: $name\n    needle: $needle\n    sqlite3:  $(printf %q "$s")\n    doltlite: $(printf %q "$d")"
  fi
}

skip() {
  SKIP=$((SKIP+1))
  echo "  SKIP: $1 — $2"
}

stock_magic() {
  want_eq "$1" "$(head -c 15 "$2" 2>/dev/null | tr -d '\0')" "SQLite format 3"
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
if ! printf '%s\n' "INSERT INTO t VALUES(2,'b');" | $DOLTLITE "$DB" >/dev/null; then
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\n  FAIL: O_wal_dl_insert (doltlite exited nonzero)"
fi
want_eq "O_wal_sqlite3_sees_doltlite_insert" \
  "$(printf '%s\n' "SELECT count(*)||group_concat(v,'') FROM (SELECT v FROM t ORDER BY id);" | $SQLITE3 "$DB" | tr -d '\r')" \
  "2ab"
oracle "O_wal_reopen_after_dl" "SELECT count(*) FROM t;" "$DB"
if ! printf '%s\n' "INSERT INTO t VALUES(3,'c');" | $SQLITE3 "$DB" >/dev/null; then
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\n  FAIL: O_wal_sqlite3_insert (sqlite3 exited nonzero)"
fi
want_eq "O_wal_doltlite_sees_sqlite3_insert" \
  "$(printf '%s\n' "SELECT count(*)||group_concat(v,'') FROM (SELECT v FROM t ORDER BY id);" | $DOLTLITE "$DB" 2>&1 | tr -d '\r' | tail -1)" \
  "3abc"
oracle "O_wal_reopen_after_sq" "SELECT count(*) FROM t;" "$DB"

echo ""
echo "--- overflow VACUUM / backup ---"

OV=$TMP/ov-dur.db
seed_stock "$OV" "PRAGMA page_size=4096; CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB, s TEXT); WITH r(i) AS (VALUES(1),(2),(3)) INSERT INTO t SELECT i, CAST(char(64+i)||substr(hex(zeroblob(25000)),1,49999) AS BLOB), char(96+i)||substr(hex(zeroblob(25000)),1,49999) FROM r;"
OVSEL="SELECT (SELECT count(DISTINCT b) FROM t)||':'||(SELECT count(DISTINCT s) FROM t);"

oracle_copy "O_vac_overflow" "VACUUM; $OVSEL" "$OV" "$OVSEL"
stock_magic "O_vac_overflow_still_stock" "$TMP/ora-d.db"
oracle "O_vac_overflow_integrity" "PRAGMA integrity_check;" "$TMP/ora-d.db"

oracle_copy "O_ov_update" "UPDATE t SET s='Z'||s WHERE id=2; $OVSEL" "$OV" "$OVSEL"

OUT=$TMP/ov-into.db
rm -f "$OUT"
if ! printf '%s\n' "VACUUM INTO '$OUT';" | $DOLTLITE "$OV" >/dev/null; then
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\n  FAIL: O_vac_into (doltlite exited nonzero)"
fi
stock_magic "O_vac_into_overflow_still_stock" "$OUT"
oracle "O_vac_into_overflow_distinct" "$OVSEL" "$OUT"
oracle "O_vac_into_overflow_integrity" "PRAGMA integrity_check;" "$OUT"
oracle "O_vac_into_source_untouched" "$OVSEL" "$OV"

DST=$TMP/ov-bak.db
seed_stock "$DST" "CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB, s TEXT); INSERT INTO t VALUES(9, x'00', 'old');"
if ! printf '.backup %s\n' "$DST" | $DOLTLITE "$OV" >/dev/null 2>&1; then
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\n  FAIL: O_backup (doltlite exited nonzero)"
fi
stock_magic "O_backup_overflow_still_stock" "$DST"
oracle "O_backup_overflow_distinct" "$OVSEL" "$DST"
oracle "O_backup_overflow_integrity" "PRAGMA integrity_check;" "$DST"
oracle "O_backup_source_untouched" "$OVSEL" "$OV"

DST2=$TMP/ov-restore.db
seed_stock "$DST2" "CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB, s TEXT); INSERT INTO t VALUES(9, x'00', 'old');"
if ! printf '.restore %s\n' "$OV" | $DOLTLITE "$DST2" >/dev/null 2>&1; then
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\n  FAIL: O_restore (doltlite exited nonzero)"
fi
stock_magic "O_restore_overflow_still_stock" "$DST2"
oracle "O_restore_overflow_distinct" "$OVSEL" "$DST2"
oracle "O_restore_overflow_integrity" "PRAGMA integrity_check;" "$DST2"

echo ""
echo "--- encodings ---"

for enc in UTF-8 UTF-16le UTF-16be; do
  DB=$TMP/enc-$enc.db
  seed_stock "$DB" "PRAGMA encoding='$enc'; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1, char(233)),(2, 'ok'),(3, char(233)||'x');"
  oracle "O_enc_${enc}_pragma" "PRAGMA encoding;" "$DB"
  oracle "O_enc_${enc}_hex_len" "SELECT group_concat(id||':'||length(v)||':'||hex(v), ',') FROM (SELECT * FROM t ORDER BY id);" "$DB"
  oracle "O_enc_${enc}_distinct" "SELECT count(DISTINCT v) FROM t;" "$DB"
done

echo ""
echo "--- WITHOUT ROWID ---"

DB=$TMP/wor.db
seed_stock "$DB" "CREATE TABLE t(k TEXT PRIMARY KEY, n INTEGER) WITHOUT ROWID; INSERT INTO t VALUES('a',1),('b',2),('c',3); CREATE TABLE u(x INTEGER PRIMARY KEY, y TEXT) WITHOUT ROWID; INSERT INTO u VALUES(1,'one'),(2,'two'),(10,'ten');"
oracle "O_wor_select" "SELECT group_concat(k||n, ',') FROM (SELECT * FROM t ORDER BY k);" "$DB"
oracle "O_wor_int_between" "SELECT count(*) FROM u WHERE x BETWEEN 2 AND 10;" "$DB"
oracle_copy "O_wor_update" "UPDATE t SET n=n+1 WHERE k='b'; SELECT group_concat(k||n, ',') FROM (SELECT * FROM t ORDER BY k);" "$DB"

echo ""
echo "--- STRICT / generated ---"

DB=$TMP/strict.db
seed_stock "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER NOT NULL) STRICT; INSERT INTO t VALUES(1,10),(2,20);"
oracle "O_strict_select" "SELECT group_concat(id||':'||n, ',') FROM (SELECT * FROM t ORDER BY id);" "$DB"
both_contain "O_strict_reject_text" "INSERT INTO t VALUES(3,'x');" "$DB" "cannot store TEXT"

DB=$TMP/gen.db
seed_stock "$DB" "CREATE TABLE g(id INTEGER PRIMARY KEY, n INT, d INT GENERATED ALWAYS AS (n*2) STORED, v INT GENERATED ALWAYS AS (n+1) VIRTUAL); INSERT INTO g(id,n) VALUES(1,5),(2,7);"
oracle "O_gen_select" "SELECT group_concat(id||':'||n||':'||d||':'||v, ',') FROM (SELECT * FROM g ORDER BY id);" "$DB"
oracle_copy "O_gen_update" "UPDATE g SET n=n+1 WHERE id=1; SELECT n||':'||d||':'||v FROM g WHERE id=1;" "$DB"

echo ""
echo "--- FTS5 / rtree ---"

if $SQLITE3 :memory: "CREATE VIRTUAL TABLE docs USING fts5(title, body);" >/dev/null 2>&1; then
  DB=$TMP/fts.db
  seed_stock "$DB" "CREATE VIRTUAL TABLE docs USING fts5(title, body); INSERT INTO docs VALUES('one','alpha beta'),('two','beta gamma');"
  oracle "O_fts5_match" "SELECT group_concat(title) FROM (SELECT title FROM docs WHERE docs MATCH 'beta' ORDER BY title);" "$DB"
else
  skip "O_fts5_match" "stock sqlite3 has no FTS5"
fi

if $SQLITE3 :memory: "CREATE VIRTUAL TABLE r USING rtree(id, minX, maxX, minY, maxY);" >/dev/null 2>&1; then
  DB=$TMP/rtree.db
  seed_stock "$DB" "CREATE VIRTUAL TABLE r USING rtree(id, minX, maxX, minY, maxY); INSERT INTO r VALUES(1,0,1,0,1),(2,5,6,5,6);"
  oracle "O_rtree_query" "SELECT group_concat(id) FROM (SELECT id FROM r WHERE minX>=0 AND maxX<=2 ORDER BY id);" "$DB"
else
  skip "O_rtree_query" "stock sqlite3 has no rtree"
fi

echo ""
echo "--- ATTACH overflow copy into a doltlite dest ---"

SRC=$TMP/ov-4096.db
if [ ! -f "$SRC" ]; then
  seed_stock "$SRC" "PRAGMA page_size=4096; CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB, s TEXT); WITH r(i) AS (VALUES(1),(2),(3)) INSERT INTO t SELECT i, CAST(char(64+i)||substr(hex(zeroblob(25000)),1,49999) AS BLOB), char(96+i)||substr(hex(zeroblob(25000)),1,49999) FROM r;"
fi

copy_overflow() {
  local distinct_name="$1" except_name="$2" ddl="$3" insert="$4" except_sql="$5"
  local dst="$TMP/copy-$distinct_name.db"
  local output
  rm -f "$dst"
  if ! output=$(printf '%s\n' \
      "ATTACH '$SRC' AS s;" \
      "$ddl" \
      "$insert" \
      "SELECT dolt_commit('-A','-m','overflow copy');" \
      | "$DOLTLITE" "$dst" 2>&1); then
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\n  FAIL: ${distinct_name%_distinct}_copy\n    output: $(printf %q "$output")"
    return
  fi
  want_eq "$distinct_name" \
    "$(printf '%s\n' "SELECT count(DISTINCT b) FROM d;" | $DOLTLITE "$dst" 2>&1 | tr -d '\r' | tail -1)" \
    "3"
  want_eq "$except_name" \
    "$(printf '%s\n' "ATTACH '$SRC' AS s; $except_sql" | $DOLTLITE "$dst" 2>&1 | tr -d '\r' | tail -1)" \
    "0:0"
}

copy_overflow \
  "O_attach_ov_textpk_distinct" "O_attach_ov_textpk_except" \
  "CREATE TABLE d(g TEXT PRIMARY KEY, id INTEGER, b BLOB);" \
  "INSERT INTO d SELECT 'g'||id, id, b FROM s.t;" \
  "SELECT (SELECT count(*) FROM (SELECT id,b FROM d EXCEPT SELECT id,b FROM s.t))||':'||(SELECT count(*) FROM (SELECT id,b FROM s.t EXCEPT SELECT id,b FROM d));"

copy_overflow \
  "O_attach_ov_intpk_distinct" "O_attach_ov_intpk_except" \
  "CREATE TABLE d(id INTEGER PRIMARY KEY, b BLOB);" \
  "INSERT INTO d SELECT id, b FROM s.t;" \
  "SELECT (SELECT count(*) FROM (SELECT id,b FROM d EXCEPT SELECT id,b FROM s.t))||':'||(SELECT count(*) FROM (SELECT id,b FROM s.t EXCEPT SELECT id,b FROM d));"

copy_overflow \
  "O_attach_ov_keyless_distinct" "O_attach_ov_keyless_except" \
  "CREATE TABLE d(id INTEGER, b BLOB);" \
  "INSERT INTO d SELECT id, b FROM s.t;" \
  "SELECT (SELECT count(*) FROM (SELECT id,b FROM d EXCEPT SELECT id,b FROM s.t))||':'||(SELECT count(*) FROM (SELECT id,b FROM s.t EXCEPT SELECT id,b FROM d));"

echo ""
echo "============================="
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
echo "============================="
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
