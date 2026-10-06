#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Non-covering index fetch by primary-key bytes ==="
echo ""

DB=/tmp/test_index_fetch_pk_bytes_$$.db

reset_db() {
  rm -f "$DB" "$(dirname "$DB")/.$(basename "$DB")-lock"
  printf '%s\n' "$1" | $DOLTLITE "$DB" > /dev/null 2>&1
}

fill() {
  echo "INSERT INTO t(id,seq,g,v) WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<600)
SELECT ${1//@/i},i,i%7,printf('v%04d',i) FROM c;
CREATE INDEX t_g ON t(g);
SELECT dolt_commit('-Am','fixture');"
}

# The index fetch must return what a scan of the table returns.
same_as_scan() {
  local name="$1" where="$2"
  run_test "$name" \
    "SELECT (SELECT group_concat(seq||':'||v, ',') FROM (SELECT seq, v FROM t INDEXED BY t_g WHERE $where ORDER BY seq))
          = (SELECT group_concat(seq||':'||v, ',') FROM (SELECT seq, v FROM t NOT INDEXED WHERE $where ORDER BY seq));" \
    "1" "$DB"
}

# record_bytes counts records rebuilt from sort keys: the fast path seeks
# with the index entry's key bytes and rebuilds none.
fetch_record_bytes() {
  run_test "$1" \
    "CREATE TEMP TABLE s0 AS SELECT value FROM dolt_engine_stats WHERE name='record_bytes';
.output /dev/null
SELECT sum(length(v)) FROM t INDEXED BY t_g WHERE g=3;
.output stdout
SELECT ((SELECT value FROM dolt_engine_stats WHERE name='record_bytes') - (SELECT value FROM s0))$2;" \
    "$3" "$DB"
}

shape() {
  local label="$1" ddl="$2" key="$3" fast="$4"
  reset_db "$ddl
$(fill "$key")"
  run_test_match "${label}_plan_fetches_through_index" \
    "EXPLAIN QUERY PLAN SELECT v FROM t INDEXED BY t_g WHERE g=3;" \
    "SEARCH t USING INDEX t_g" "$DB"
  same_as_scan "${label}_fetch_matches_scan" "g=3"
  same_as_scan "${label}_range_fetch_matches_scan" "g BETWEEN 2 AND 4"
  if [ "$fast" = fast ]; then
    fetch_record_bytes "${label}_fetch_rebuilds_no_record" "" "0"
  fi
  run_test "${label}_pending_edits_fetch_matches_scan" \
    "BEGIN;
UPDATE t SET g=3 WHERE seq%11=0;
DELETE FROM t WHERE seq%13=0;
INSERT INTO t(id,seq,g,v) SELECT ${key//@/(seq+1000)},seq+1000,3,'new'||seq FROM t WHERE seq<40;
UPDATE t SET v='upd' WHERE seq%17=0;
SELECT (SELECT group_concat(seq||':'||v, ',') FROM (SELECT seq, v FROM t INDEXED BY t_g WHERE g=3 ORDER BY seq))
     = (SELECT group_concat(seq||':'||v, ',') FROM (SELECT seq, v FROM t NOT INDEXED WHERE g=3 ORDER BY seq));
ROLLBACK;" \
    "1" "$DB"
  run_test "${label}_integrity" "PRAGMA integrity_check;" "ok" "$DB"
}

shape text \
  "CREATE TABLE t(id TEXT PRIMARY KEY, seq INTEGER NOT NULL, g INTEGER, v TEXT);" \
  "printf('%016x',@)" fast
shape blob \
  "CREATE TABLE t(id BLOB PRIMARY KEY, seq INTEGER NOT NULL, g INTEGER, v TEXT);" \
  "CAST(printf('%016x',@) AS BLOB)" fast
shape composite \
  "CREATE TABLE t(id TEXT NOT NULL, seq INTEGER NOT NULL, g INTEGER, v TEXT, PRIMARY KEY(id, seq));" \
  "printf('%016x',@)" fast
shape without_rowid_composite \
  "CREATE TABLE t(id TEXT NOT NULL, seq INTEGER NOT NULL, g INTEGER, v TEXT, PRIMARY KEY(id, seq)) WITHOUT ROWID;" \
  "printf('%016x',@)" fast
shape integer_then_desc \
  "CREATE TABLE t(id INTEGER NOT NULL, seq INTEGER NOT NULL, g INTEGER, v TEXT, PRIMARY KEY(id, seq DESC));" \
  "@/3" fast
# The index carries the key column's DESC order, so its bytes still match.
shape desc_key \
  "CREATE TABLE t(id TEXT NOT NULL, seq INTEGER NOT NULL, g INTEGER, v TEXT, PRIMARY KEY(id DESC));" \
  "printf('%016x',@)" fast
shape nocase_key \
  "CREATE TABLE t(id TEXT COLLATE NOCASE PRIMARY KEY, seq INTEGER NOT NULL, g INTEGER, v TEXT);" \
  "printf('%016X',@)" fallback

# A key column that the index already holds is not repeated at the end of
# the entry, so its trailing bytes are not the table key.
reset_db "CREATE TABLE t(id TEXT NOT NULL, seq INTEGER NOT NULL, g INTEGER, v TEXT, PRIMARY KEY(id, seq));
INSERT INTO t(id,seq,g,v) WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<600)
SELECT printf('%016x',i),i,i%7,printf('v%04d',i) FROM c;
CREATE INDEX t_g ON t(g, seq);
SELECT dolt_commit('-Am','fixture');"
same_as_scan "key_column_in_index_fetch_matches_scan" "g=3"
fetch_record_bytes "key_column_in_index_falls_back" ">0" "1"

rm -f "$DB" "$(dirname "$DB")/.$(basename "$DB")-lock"
dltest_finish
