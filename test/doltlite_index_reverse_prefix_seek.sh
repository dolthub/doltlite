#!/bin/bash
# SeekLE/SeekGT prefix seeks must land on the last matching entry directly, not walk the run.
DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== reverse prefix seeks land on the last match ==="
echo ""

ROOT=$(mktemp -d /tmp/dl_rev_prefix_XXXXXX)
trap 'rm -rf "$ROOT"' EXIT
DB="$ROOT/rp.db"

# Every check compares the indexed plan against the same query forced through a table scan.
same() {
  local name="$1" sql="$2" scan="$3"
  run_test "$name" "SELECT ($sql) IS ($scan);" "1" "$DB"
}

run_test "seed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, s TEXT, r REAL);
WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<3000)
INSERT INTO t(a,b,s,r) SELECT x%7, x, 'k'||(x%13), x/10.0 FROM c;
INSERT INTO t(a,b,s,r) VALUES(2,NULL,NULL,NULL),(2,NULL,'z',1.5),(9007199254740992,1,'x',1),(9007199254740993,2,'x',1),(9007199254740994,3,'x',1),(9007199254740995,4,'x',1),(-9007199254740993,5,'x',1),(-9007199254740992,6,'x',1);
CREATE INDEX iab ON t(a,b);
CREATE INDEX iabd ON t(a,b DESC);
CREATE INDEX ias ON t(a,s);
CREATE INDEX iasd ON t(a DESC,s DESC);
CREATE INDEX iar ON t(a,r);
CREATE INDEX isb ON t(s,b);
CREATE INDEX isbn ON t(s COLLATE NOCASE,b);
SELECT count(*) FROM t;
" "3008" "$DB"

for a in 0 1 3 6 2 5 9007199254740992 9007199254740993 9007199254740994 9007199254740995 -9007199254740993 -9007199254740992 99; do
  same "max_b_a_$a" "SELECT max(b) FROM t INDEXED BY iab WHERE a=$a" "SELECT max(b) FROM t NOT INDEXED WHERE a=$a"
  same "desc_limit_a_$a" "SELECT b FROM t INDEXED BY iab WHERE a=$a ORDER BY b DESC LIMIT 1" "SELECT b FROM t NOT INDEXED WHERE a=$a ORDER BY b DESC LIMIT 1"
  same "min_b_desc_idx_a_$a" "SELECT min(b) FROM t INDEXED BY iabd WHERE a=$a" "SELECT min(b) FROM t NOT INDEXED WHERE a=$a"
  same "max_b_desc_idx_a_$a" "SELECT max(b) FROM t INDEXED BY iabd WHERE a=$a" "SELECT max(b) FROM t NOT INDEXED WHERE a=$a"
  same "max_s_a_$a" "SELECT max(s) FROM t INDEXED BY ias WHERE a=$a" "SELECT max(s) FROM t NOT INDEXED WHERE a=$a"
  same "min_s_descdesc_a_$a" "SELECT min(s) FROM t INDEXED BY iasd WHERE a=$a" "SELECT min(s) FROM t NOT INDEXED WHERE a=$a"
  same "max_r_a_$a" "SELECT max(r) FROM t INDEXED BY iar WHERE a=$a" "SELECT max(r) FROM t NOT INDEXED WHERE a=$a"
  same "gt_prefix_a_$a" "SELECT group_concat(id) FROM (SELECT id FROM t INDEXED BY iab WHERE a>$a ORDER BY a,b,id LIMIT 3)" "SELECT group_concat(id) FROM (SELECT id FROM t NOT INDEXED WHERE a>$a ORDER BY a,b,id LIMIT 3)"
  same "le_prefix_a_$a" "SELECT group_concat(id) FROM (SELECT id FROM t INDEXED BY iab WHERE a<=$a ORDER BY a DESC,b DESC,id DESC LIMIT 3)" "SELECT group_concat(id) FROM (SELECT id FROM t NOT INDEXED WHERE a<=$a ORDER BY a DESC,b DESC,id DESC LIMIT 3)"
done
same "max_b_a_real" "SELECT max(b) FROM t INDEXED BY iab WHERE a=2.0" "SELECT max(b) FROM t NOT INDEXED WHERE a=2.0"
same "max_b_a_null" "SELECT max(b) FROM t INDEXED BY iab WHERE a IS NULL" "SELECT max(b) FROM t NOT INDEXED WHERE a IS NULL"
same "two_field_prefix_max" "SELECT max(id) FROM t INDEXED BY iab WHERE a=3 AND b=10" "SELECT max(id) FROM t NOT INDEXED WHERE a=3 AND b=10"
same "text_prefix_max" "SELECT max(b) FROM t INDEXED BY isb WHERE s='k5'" "SELECT max(b) FROM t NOT INDEXED WHERE s='k5'"
same "text_prefix_desc_limit" "SELECT b FROM t INDEXED BY isb WHERE s='k12' ORDER BY b DESC LIMIT 1" "SELECT b FROM t NOT INDEXED WHERE s='k12' ORDER BY b DESC LIMIT 1"
same "nocase_prefix_max" "SELECT max(b) FROM t INDEXED BY isbn WHERE s='K5' COLLATE NOCASE" "SELECT max(b) FROM t NOT INDEXED WHERE s='K5' COLLATE NOCASE"
same "text_missing_prefix" "SELECT max(b) FROM t INDEXED BY isb WHERE s='nope'" "SELECT max(b) FROM t NOT INDEXED WHERE s='nope'"
same "text_null_prefix" "SELECT max(b) FROM t INDEXED BY isb WHERE s IS NULL" "SELECT max(b) FROM t NOT INDEXED WHERE s IS NULL"
same "range_desc_scan" "SELECT group_concat(b) FROM (SELECT b FROM t INDEXED BY iab WHERE a=4 AND b BETWEEN 100 AND 200 ORDER BY b DESC)" "SELECT group_concat(b) FROM (SELECT b FROM t NOT INDEXED WHERE a=4 AND b BETWEEN 100 AND 200 ORDER BY b DESC)"

# Pending map: rows inserted, deleted and moved inside the transaction that runs the seeks.
run_test "pending_map_matrix" "
BEGIN;
INSERT INTO t(a,b,s,r) VALUES(1,999999,'zz',1),(1,-5,'aa',1),(4,999999,'zz',1),(8,1,'p',1),(8,2,'q',1);
DELETE FROM t WHERE a=3 AND b>2900;
DELETE FROM t WHERE a=5;
UPDATE t SET b=b+1000000 WHERE a=6 AND b>2990;
INSERT INTO t(a,b,s,r) VALUES(9007199254740993,7,'x',1),(9007199254740992,8,'x',1);
SELECT (SELECT max(b) FROM t INDEXED BY iab WHERE a=1) IS (SELECT max(b) FROM t NOT INDEXED WHERE a=1);
SELECT (SELECT min(b) FROM t INDEXED BY iabd WHERE a=1) IS (SELECT min(b) FROM t NOT INDEXED WHERE a=1);
SELECT (SELECT max(b) FROM t INDEXED BY iab WHERE a=3) IS (SELECT max(b) FROM t NOT INDEXED WHERE a=3);
SELECT (SELECT max(b) FROM t INDEXED BY iab WHERE a=5) IS (SELECT max(b) FROM t NOT INDEXED WHERE a=5);
SELECT (SELECT max(b) FROM t INDEXED BY iab WHERE a=6) IS (SELECT max(b) FROM t NOT INDEXED WHERE a=6);
SELECT (SELECT max(b) FROM t INDEXED BY iab WHERE a=8) IS (SELECT max(b) FROM t NOT INDEXED WHERE a=8);
SELECT (SELECT max(b) FROM t INDEXED BY iab WHERE a=4) IS (SELECT max(b) FROM t NOT INDEXED WHERE a=4);
SELECT (SELECT max(b) FROM t INDEXED BY iab WHERE a=9007199254740992) IS (SELECT max(b) FROM t NOT INDEXED WHERE a=9007199254740992);
SELECT (SELECT max(b) FROM t INDEXED BY iab WHERE a=9007199254740993) IS (SELECT max(b) FROM t NOT INDEXED WHERE a=9007199254740993);
SELECT (SELECT group_concat(id) FROM (SELECT id FROM t INDEXED BY iab WHERE a>5 ORDER BY a,b,id LIMIT 4)) IS (SELECT group_concat(id) FROM (SELECT id FROM t NOT INDEXED WHERE a>5 ORDER BY a,b,id LIMIT 4));
SELECT (SELECT group_concat(id) FROM (SELECT id FROM t INDEXED BY iab WHERE a<=5 ORDER BY a DESC,b DESC,id DESC LIMIT 4)) IS (SELECT group_concat(id) FROM (SELECT id FROM t NOT INDEXED WHERE a<=5 ORDER BY a DESC,b DESC,id DESC LIMIT 4));
SELECT (SELECT max(s) FROM t INDEXED BY ias WHERE a=1) IS (SELECT max(s) FROM t NOT INDEXED WHERE a=1);
ROLLBACK;
" "1
1
1
1
1
1
1
1
1
1
1
1" "$DB"

run_test "pending_only_table" "
BEGIN;
CREATE TABLE p(a INT, b INT);
CREATE INDEX pab ON p(a,b);
INSERT INTO p VALUES(1,1),(1,2),(1,3),(2,1),(9007199254740992,1),(9007199254740993,2);
SELECT max(b) FROM p INDEXED BY pab WHERE a=1;
SELECT max(b) FROM p INDEXED BY pab WHERE a=2;
SELECT max(b) FROM p INDEXED BY pab WHERE a=3;
SELECT max(b) FROM p INDEXED BY pab WHERE a=9007199254740992;
SELECT max(b) FROM p INDEXED BY pab WHERE a=9007199254740993;
DELETE FROM p WHERE a=1 AND b=3;
SELECT max(b) FROM p INDEXED BY pab WHERE a=1;
ROLLBACK;
" "3
1

1
2
2" "$DB"

run_test "clustered_composite_pk_prefix" "
CREATE TABLE c(a INT, b INT, v TEXT, PRIMARY KEY(a,b));
WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x<2000) INSERT INTO c SELECT x%5, x, 'v' FROM s;
SELECT (SELECT max(b) FROM c WHERE a=2) IS (SELECT max(b) FROM c NOT INDEXED WHERE a=2);
SELECT (SELECT b FROM c WHERE a=4 ORDER BY b DESC LIMIT 1) IS (SELECT b FROM c NOT INDEXED WHERE a=4 ORDER BY b DESC LIMIT 1);
SELECT (SELECT min(b) FROM c WHERE a=0) IS (SELECT min(b) FROM c NOT INDEXED WHERE a=0);
" "1
1
1" "$DB"

# 400k rows sharing one prefix: 20 reverse seeks must not cost a full walk each.
BIG="$ROOT/big.db"
run_test "big_seed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x<400000) INSERT INTO t(a,b) SELECT 1, x FROM s;
CREATE INDEX iab ON t(a,b);
SELECT max(b) FROM t WHERE a=1;
" "400000" "$BIG"
q=""; for i in $(seq 1 20); do q="$q SELECT max(b) FROM t WHERE a=1;"; done
secs=$(printf '.timer on\n%s\n' "$q" | "$DOLTLITE" "$BIG" 2>&1 | awk '/Run Time:/{t+=$4} END{printf "%.4f", t}')
if awk -v s="$secs" 'BEGIN{exit !(s<0.1)}'; then
  dltest_pass
else
  dltest_fail "reverse_prefix_seek_is_log_n" "  20 max(b) seeks over a 400000-row prefix took ${secs}s (limit 0.1s)"
fi

dltest_finish
