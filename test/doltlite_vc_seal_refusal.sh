#!/bin/bash
# A VC command inside BEGIN whose closing COMMIT is refused must change
# nothing: no ref move, no working-set change, no partial reset.
DLTEST_STRIP_CR=1
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== VC commands refused at the seal leave refs and rows unchanged ==="
echo ""

ROOT=$(mktemp -d ./.doltlite-vc-seal.XXXXXX)
trap 'rm -rf "$ROOT"' EXIT

STATE="SELECT group_concat(message, '|') FROM dolt_log;
SELECT group_concat(name, ',') FROM (SELECT name FROM dolt_branches ORDER BY name);
SELECT group_concat(a, ',') FROM (SELECT a FROM t ORDER BY a);
SELECT count(*) FROM dolt_status;"
WANT="main c2|main c1|Initialize data repository
main,o
5
0"

seed() {
  rm -f "$1"
  "$DOLTLITE" "$1" >/dev/null 2>&1 <<'EOF'
CREATE TABLE p(id PRIMARY KEY);
CREATE TABLE c(id PRIMARY KEY, pid REFERENCES p(id));
CREATE TABLE t(a PRIMARY KEY);
INSERT INTO p VALUES(1);
SELECT dolt_commit('-Am','main c1');
SELECT dolt_checkout('-b','o');
INSERT INTO t VALUES(100);
SELECT dolt_commit('-am','o1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5);
SELECT dolt_commit('-am','main c2');
EOF
}

for op in "dolt_cherry_pick('o')" "dolt_revert('HEAD')" \
          "dolt_reset('--hard','HEAD~1')" "dolt_reset('--soft','HEAD~1')" \
          "dolt_branch('nb')" "dolt_branch('-m','o','o2')" "dolt_branch('-D','o')"; do
  DB="$ROOT/fk.db"
  seed "$DB"
  out=$("$DOLTLITE" "$DB" 2>&1 <<EOF
PRAGMA foreign_keys=ON;
BEGIN;
PRAGMA defer_foreign_keys=ON;
INSERT INTO c VALUES(1, 999);
SELECT $op;
ROLLBACK;
EOF
)
  dltest_assert_match "deferred_fk_refuses: $op" "$out" "^Error"
  run_test "deferred_fk_unchanged: $op" "$STATE" "$WANT" "$DB"

  case "$op" in dolt_branch*|*--soft*) continue ;; esac
  DB="$ROOT/att.db"
  AUX="$ROOT/aux.db"
  seed "$DB"
  rm -f "$AUX"
  "$DOLTLITE" "$AUX" "CREATE TABLE x(a PRIMARY KEY); INSERT INTO x VALUES(1);" >/dev/null 2>&1
  out=$("$DOLTLITE" "$DB" 2>&1 <<EOF
ATTACH '$AUX' AS aux;
BEGIN;
INSERT INTO aux.x VALUES(3);
SELECT $op;
COMMIT;
EOF
)
  dltest_assert_match "attach_refuses: $op" "$out" "multiple file-backed databases"
  run_test "attach_unchanged: $op" "$STATE" "$WANT" "$DB"
done

dltest_finish
