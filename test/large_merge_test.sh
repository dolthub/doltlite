#!/bin/bash
#
# Large merge tests — merge correctness at scale (10K+ rows), exercising the
# streaming three-way diff and the merge-apply path on multi-level prolly
# trees. Rows are built with recursive CTEs piped on stdin; the previous
# version passed thousands of generated INSERTs as a single shell argument
# and silently died with "Argument list too long", so it never actually ran.
#
# Usage: bash test/large_merge_test.sh [path/to/doltlite]

set -u
DOLTLITE="${1:-./doltlite}"
TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT
pass=0; fail=0; errors=""

# Run SQL (stdin) against a db; echo last stdout line.
dl() { printf '%s\n' "$2" | "$DOLTLITE" "$1" 2>&1 | tail -1; }
run() { printf '%s\n' "$2" | "$DOLTLITE" "$1" >/dev/null 2>&1; }

check() {
  local name="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    pass=$((pass+1)); echo "  PASS: $name"
  else
    fail=$((fail+1)); errors="$errors\n  FAIL: $name (want=$want got=$got)"
    echo "  FAIL: $name (want=$want got=$got)"
  fi
}

# seq(lo,hi) recursive-CTE fragment producing column i.
cte() { echo "WITH RECURSIVE c(i) AS (SELECT $1 UNION ALL SELECT i+1 FROM c WHERE i<$2)"; }

echo "=== Large Merge Tests ==="

# ── S1: non-overlapping inserts — feat and main add disjoint 10K ranges ──
echo "--- S1: non-overlapping inserts (10K + 10K) ---"
DB="$TMP/s1.db"; rm -f "$DB"
run "$DB" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t(id,v) $(cte 1 1000) SELECT i,'base_'||i FROM c;
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat'); SELECT dolt_checkout('feat');
INSERT INTO t(id,v) $(cte 1001 11000) SELECT i,'feat_'||i FROM c;
SELECT dolt_commit('-A','-m','feat +10k');
SELECT dolt_checkout('main');
INSERT INTO t(id,v) $(cte 11001 21000) SELECT i,'main_'||i FROM c;
SELECT dolt_commit('-A','-m','main +10k');
SELECT dolt_merge('feat');"
check "s1_count"        "$(dl "$DB" "SELECT count(*) FROM t;")" "21000"
check "s1_feat_value"   "$(dl "$DB" "SELECT v FROM t WHERE id=5000;")" "feat_5000"
check "s1_main_value"   "$(dl "$DB" "SELECT v FROM t WHERE id=15000;")" "main_15000"
check "s1_no_conflicts" "$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")" "0"

# ── S2: disjoint updates — feat updates odd ids, main updates even ids ──
echo "--- S2: disjoint updates of 10K base rows ---"
DB="$TMP/s2.db"; rm -f "$DB"
run "$DB" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t(id,v) $(cte 1 10000) SELECT i,'base_'||i FROM c;
SELECT dolt_commit('-A','-m','base 10k');
SELECT dolt_branch('feat'); SELECT dolt_checkout('feat');
UPDATE t SET v='feat_'||id WHERE id%2=1;
SELECT dolt_commit('-A','-m','feat odd');
SELECT dolt_checkout('main');
UPDATE t SET v='main_'||id WHERE id%2=0;
SELECT dolt_commit('-A','-m','main even');
SELECT dolt_merge('feat');"
check "s2_count"         "$(dl "$DB" "SELECT count(*) FROM t;")" "10000"
check "s2_no_conflicts"  "$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")" "0"
check "s2_odd_from_feat" "$(dl "$DB" "SELECT v FROM t WHERE id=4001;")" "feat_4001"
check "s2_even_from_main" "$(dl "$DB" "SELECT v FROM t WHERE id=4000;")" "main_4000"

# ── S3: conflicting updates — both sides change the SAME 5K rows ──
echo "--- S3: conflicting updates (5K rows, both sides) ---"
DB="$TMP/s3.db"; rm -f "$DB"
run "$DB" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t(id,v) $(cte 1 5000) SELECT i,'base_'||i FROM c;
SELECT dolt_commit('-A','-m','base 5k');
SELECT dolt_branch('feat'); SELECT dolt_checkout('feat');
UPDATE t SET v='feat_'||id;
SELECT dolt_commit('-A','-m','feat all');
SELECT dolt_checkout('main');
UPDATE t SET v='main_'||id;
SELECT dolt_commit('-A','-m','main all');"
# Conflicts are visible within the merge transaction; autocommit would roll
# back. dolt_conflicts is a per-table summary (count(*) would be 1 = one table
# with conflicts); the conflicting rows live in dolt_conflicts_<table>.
TX=$(printf '%s\n' "BEGIN; SELECT dolt_merge('feat');
SELECT 'TX|' || (SELECT count(*) FROM dolt_conflicts_t);
ROLLBACK;" | "$DOLTLITE" "$DB" 2>&1 | grep '^TX|')
check "s3_conflicts_detected" "$TX" "TX|5000"
# Autocommit merge of the same conflict rolls the whole thing back.
run "$DB" "SELECT dolt_merge('feat');"
check "s3_autocommit_rolled_back" "$(dl "$DB" "SELECT count(*) FROM dolt_conflicts;")" "0"
check "s3_value_restored" "$(dl "$DB" "SELECT v FROM t WHERE id=2500;")" "main_2500"

echo ""
echo "Results: $pass passed, $fail failed out of $((pass+fail)) tests"
if [ "$fail" -ne 0 ]; then
  printf '%b\n' "$errors"
  exit 1
fi
