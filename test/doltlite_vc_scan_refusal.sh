#!/bin/bash
# A VC command called from a row of a table scan either leaves that scan
# readable to the end or is refused before it changes anything.
DLTEST_STRIP_CR=1
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== VC commands called from a table scan ==="
echo ""

ROOT=$(mktemp -d ./.doltlite-vc-scan.XXXXXX)
trap 'rm -rf "$ROOT"' EXIT

STATE="SELECT group_concat(message, '|') FROM dolt_log;
SELECT active_branch() || ':' || (SELECT count(*) FROM t) || ':' || (SELECT count(*) FROM dolt_status);"
WANT="c2|c1|Initialize data repository
main:3:0"
REFUSED="while the same statement is reading a table it would replace"

seed() {
  rm -f "$1"
  "$DOLTLITE" "$1" >/dev/null 2>&1 <<'EOF'
CREATE TABLE t(a INTEGER PRIMARY KEY, b);
CREATE INDEX tb ON t(b);
CREATE TABLE u(x);
INSERT INTO t VALUES(1,1),(2,2);
SELECT dolt_commit('-Am','c1');
SELECT dolt_checkout('-b','o');
INSERT INTO u VALUES(1);
SELECT dolt_commit('-am','o1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,4);
SELECT dolt_commit('-am','c2');
EOF
}

for scan in "FROM t" "FROM t INDEXED BY tb WHERE b>0" "FROM t, t AS t2"; do
  for op in "dolt_merge('o')" "dolt_cherry_pick('o')" "dolt_revert('HEAD')" \
            "dolt_reset('--hard','HEAD~1')" "dolt_checkout('o')" \
            "dolt_rebase('o')"; do
    DB="$ROOT/scan.db"
    seed "$DB"
    out=$("$DOLTLITE" "$DB" "SELECT t.a, $op $scan;" 2>&1)
    dltest_assert_match "refused: $op $scan" "$out" "$REFUSED"
    run_test "unchanged: $op $scan" "$STATE" "$WANT" "$DB"

    seed "$DB"
    out=$("$DOLTLITE" "$DB" 2>&1 <<EOF
BEGIN;
SELECT 'n', count(*) FROM u;
SELECT t.a, $op $scan;
ROLLBACK;
EOF
)
    dltest_assert_match "refused in BEGIN: $op $scan" "$out" "$REFUSED"
    run_test "unchanged in BEGIN: $op $scan" "$STATE" "$WANT" "$DB"
  done
done

DB="$ROOT/dirty.db"
seed "$DB"
out=$("$DOLTLITE" "$DB" 2>&1 <<'EOF'
BEGIN;
INSERT INTO t VALUES(9,9);
SELECT a, dolt_reset('--hard') FROM t;
SELECT 'rows', count(*) FROM t;
ROLLBACK;
EOF
)
dltest_assert_match "dirty reset in BEGIN refused" "$out" "$REFUSED"
dltest_assert_match "dirty reset in BEGIN keeps the row" "$out" "^rows\|4$"

seed "$DB"
run_test "clean reset finishes the scan" \
  "SELECT a, dolt_reset('--hard') FROM t;" "1|0
2|0
4|0" "$DB"
run_test "clean dolt_clean finishes the scan" \
  "SELECT a, dolt_clean() FROM t;" "1|0
2|0
4|0" "$DB"
run_test "branch per row" \
  "CREATE TABLE names(n); INSERT INTO names VALUES('x1'),('x2');
SELECT n, dolt_branch(n) FROM names;
SELECT count(*) FROM dolt_branches WHERE name IN ('x1','x2');" "x1|0
x2|0
2" "$DB"
run_test "checkout per dolt_branches row" \
  "SELECT name, dolt_checkout(name) FROM dolt_branches WHERE name='o';
SELECT active_branch();" "o|0
o" "$DB"

dltest_finish
