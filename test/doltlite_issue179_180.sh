#!/bin/bash
#
# Regression tests for GitHub issues #179 and #180
#
# #180: MAX() aggregate with WHERE clause returns wrong result
# #179: Self-referencing subquery in INSERT doesn't see own transaction's writes
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(echo "$s" | perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

echo "=== Issue #179 / #180 Regression Tests ==="
echo ""

# ============================================================
# Issue #180: MAX() with WHERE clause
# ============================================================

DB180=/tmp/test_issue180_$$.db; rm -f "$DB180"

# Setup: create table with composite unique index and insert rows
echo "CREATE TABLE events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  aggregate_kind TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  stream_version INTEGER NOT NULL,
  data TEXT,
  UNIQUE(aggregate_kind, stream_id, stream_version)
);
-- Insert 10 rows for one stream (versions 0-9)
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',0,'v0');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',1,'v1');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',2,'v2');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',3,'v3');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',4,'v4');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',5,'v5');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',6,'v6');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',7,'v7');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',8,'v8');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',9,'v9');
-- Also insert rows for a different stream
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s2',0,'other');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s2',1,'other');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('cmd','s1',0,'other_kind');" \
  | $DOLTLITE "$DB180" > /dev/null 2>&1

# Test: MAX with WHERE should return 9
run_test "issue180_max_where" \
  "SELECT MAX(stream_version) FROM events WHERE aggregate_kind='thread' AND stream_id='s1';" \
  "9" "$DB180"

# Test: COUNT with same WHERE should return 10
run_test "issue180_count_where" \
  "SELECT COUNT(*) FROM events WHERE aggregate_kind='thread' AND stream_id='s1';" \
  "10" "$DB180"

# Test: ORDER BY DESC LIMIT 1 with WHERE
run_test "issue180_orderby_desc" \
  "SELECT stream_version FROM events WHERE aggregate_kind='thread' AND stream_id='s1' ORDER BY stream_version DESC LIMIT 1;" \
  "9" "$DB180"

# Test: MAX via GROUP BY HAVING (workaround - should work)
run_test "issue180_max_groupby_having" \
  "SELECT MAX(stream_version) FROM events GROUP BY aggregate_kind, stream_id HAVING aggregate_kind='thread' AND stream_id='s1';" \
  "9" "$DB180"

# Test: MAX on the other stream
run_test "issue180_max_where_s2" \
  "SELECT MAX(stream_version) FROM events WHERE aggregate_kind='thread' AND stream_id='s2';" \
  "1" "$DB180"

# Test: COALESCE(MAX()+1, 0) pattern used by event sourcing
run_test "issue180_coalesce_max" \
  "SELECT COALESCE(MAX(stream_version)+1, 0) FROM events WHERE aggregate_kind='thread' AND stream_id='s1';" \
  "10" "$DB180"

# Test: MIN with WHERE (should also work)
run_test "issue180_min_where" \
  "SELECT MIN(stream_version) FROM events WHERE aggregate_kind='thread' AND stream_id='s1';" \
  "0" "$DB180"

# Test with more rows to make it span multiple leaf nodes
DB180_LARGE=/tmp/test_issue180_large_$$.db; rm -f "$DB180_LARGE"
echo "CREATE TABLE events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  aggregate_kind TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  stream_version INTEGER NOT NULL,
  data TEXT,
  UNIQUE(aggregate_kind, stream_id, stream_version)
);" | $DOLTLITE "$DB180_LARGE" > /dev/null 2>&1

# Insert 500 rows
{
  echo "BEGIN;"
  for i in $(seq 0 499); do
    echo "INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',$i,'data_$i');"
  done
  echo "COMMIT;"
} | $DOLTLITE "$DB180_LARGE" > /dev/null 2>&1

run_test "issue180_max_where_500rows" \
  "SELECT MAX(stream_version) FROM events WHERE aggregate_kind='thread' AND stream_id='s1';" \
  "499" "$DB180_LARGE"

run_test "issue180_count_where_500rows" \
  "SELECT COUNT(*) FROM events WHERE aggregate_kind='thread' AND stream_id='s1';" \
  "500" "$DB180_LARGE"

# ============================================================
# Issue #179: Self-referencing subquery in INSERT
# ============================================================

DB179=/tmp/test_issue179_$$.db; rm -f "$DB179"

# Repro from the issue
echo "CREATE TABLE t(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT, sid TEXT, ver INTEGER,
  UNIQUE(kind, sid, ver)
);" | $DOLTLITE "$DB179" > /dev/null 2>&1

# Test: 3 self-referencing inserts in a transaction should all succeed
run_test "issue179_self_ref_insert" \
  "BEGIN;
INSERT INTO t(kind,sid,ver) VALUES('thread','t1',
  COALESCE((SELECT ver+1 FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver DESC LIMIT 1), 0));
INSERT INTO t(kind,sid,ver) VALUES('thread','t1',
  COALESCE((SELECT ver+1 FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver DESC LIMIT 1), 0));
INSERT INTO t(kind,sid,ver) VALUES('thread','t1',
  COALESCE((SELECT ver+1 FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver DESC LIMIT 1), 0));
COMMIT;
SELECT ver FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver;" \
  "0
1
2" "$DB179"

# Test: 5 self-referencing inserts in a transaction
DB179_5=/tmp/test_issue179_5_$$.db; rm -f "$DB179_5"
echo "CREATE TABLE t(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT, sid TEXT, ver INTEGER,
  UNIQUE(kind, sid, ver)
);" | $DOLTLITE "$DB179_5" > /dev/null 2>&1

run_test "issue179_self_ref_5inserts" \
  "BEGIN;
INSERT INTO t(kind,sid,ver) VALUES('thread','t1',
  COALESCE((SELECT ver+1 FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver DESC LIMIT 1), 0));
INSERT INTO t(kind,sid,ver) VALUES('thread','t1',
  COALESCE((SELECT ver+1 FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver DESC LIMIT 1), 0));
INSERT INTO t(kind,sid,ver) VALUES('thread','t1',
  COALESCE((SELECT ver+1 FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver DESC LIMIT 1), 0));
INSERT INTO t(kind,sid,ver) VALUES('thread','t1',
  COALESCE((SELECT ver+1 FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver DESC LIMIT 1), 0));
INSERT INTO t(kind,sid,ver) VALUES('thread','t1',
  COALESCE((SELECT ver+1 FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver DESC LIMIT 1), 0));
COMMIT;
SELECT ver FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver;" \
  "0
1
2
3
4" "$DB179_5"

# Test: Self-referencing insert using MAX (the event sourcing pattern)
DB179_MAX=/tmp/test_issue179_max_$$.db; rm -f "$DB179_MAX"
echo "CREATE TABLE events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  aggregate_kind TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  stream_version INTEGER NOT NULL,
  data TEXT,
  UNIQUE(aggregate_kind, stream_id, stream_version)
);" | $DOLTLITE "$DB179_MAX" > /dev/null 2>&1

run_test "issue179_max_pattern" \
  "BEGIN;
INSERT INTO events(aggregate_kind,stream_id,stream_version,data) VALUES('thread','s1',
  COALESCE((SELECT MAX(stream_version)+1 FROM events WHERE aggregate_kind='thread' AND stream_id='s1'), 0), 'e0');
INSERT INTO events(aggregate_kind,stream_id,stream_version,data) VALUES('thread','s1',
  COALESCE((SELECT MAX(stream_version)+1 FROM events WHERE aggregate_kind='thread' AND stream_id='s1'), 0), 'e1');
INSERT INTO events(aggregate_kind,stream_id,stream_version,data) VALUES('thread','s1',
  COALESCE((SELECT MAX(stream_version)+1 FROM events WHERE aggregate_kind='thread' AND stream_id='s1'), 0), 'e2');
COMMIT;
SELECT stream_version FROM events WHERE aggregate_kind='thread' AND stream_id='s1' ORDER BY stream_version;" \
  "0
1
2" "$DB179_MAX"

# ============================================================
# Summary
# ============================================================

rm -f "$DB180" "$DB180_LARGE" "$DB179" "$DB179_5" "$DB179_MAX"

echo ""
echo "Results: $PASS passed, $FAIL failed"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
echo "All tests passed!"
