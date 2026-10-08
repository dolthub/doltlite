#!/bin/bash
# A VC command inside a transaction that has only read must not overwrite a
# peer write committed after the snapshot.
DLTEST_STRIP_CR=1
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== VC commands in a stale read transaction keep peer writes ==="
echo ""

ROOT=$(mktemp -d ./.doltlite-stale-vc.XXXXXX)
trap 'rm -rf "$ROOT"' EXIT
SHELL_DOLTLITE="$DOLTLITE"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) SHELL_DOLTLITE=$(cygpath -am "$DOLTLITE") ;;
esac

for peer in conn proc; do
for begin in "BEGIN" "SAVEPOINT s"; do
  for op in "dolt_clean()" "dolt_reset('other_t')" "dolt_checkout('other_t')" \
            "dolt_revert('HEAD')" "dolt_cherry_pick('b1')"; do
    DB="$ROOT/db"
    rm -f "$DB"
    case "$op" in
      dolt_revert*|dolt_cherry_pick*) dirty=""; n=1 ;;
      *) dirty="INSERT INTO other_t VALUES(5);"; n=2 ;;
    esac
    "$DOLTLITE" "$DB" "
CREATE TABLE acct(id INTEGER PRIMARY KEY, bal INT);
CREATE TABLE other_t(x);
INSERT INTO acct VALUES(1,100);
SELECT dolt_commit('-Am','c1');
INSERT INTO other_t VALUES(1);
SELECT dolt_commit('-am','c2');
SELECT dolt_checkout('-b','b1');
INSERT INTO other_t VALUES(2);
SELECT dolt_commit('-am','c3');
SELECT dolt_checkout('main');
$dirty
" >/dev/null 2>&1
    if [ "$peer" = conn ]; then
      peer_write=".connection 1
.open $DB
UPDATE acct SET bal=bal+500;
.connection 0"
    else
      peer_write=".shell $DOLTLITE $DB 'UPDATE acct SET bal=bal+500;'"
    fi
    out=$("$DOLTLITE" "$DB" 2>&1 <<SQL
$begin;
SELECT count(*) FROM other_t;
$peer_write
SELECT $op;
COMMIT;
SELECT 'session', bal, (SELECT count(*) FROM other_t) FROM acct;
SQL
)
    label="$peer $(echo "$begin" | tr -cd '[:alpha:]'): $op"
    run_test "stale_txn_keeps_peer_write $label" \
      "PRAGMA integrity_check; SELECT bal, (SELECT count(*) FROM other_t) FROM acct;" \
      "ok
600|$n" "$DB"
    case "$out" in
      *"session|600|$n"*) dltest_pass "stale_txn_session_reloads $label" ;;
      *) dltest_fail "stale_txn_session_reloads $label" "  got: $out" ;;
    esac
  done
done
done

for peer in conn proc; do
  for maintenance in none gc vacuum; do
    for begin in "BEGIN" "SAVEPOINT s"; do
      DB="$ROOT/checkout_${peer}_${maintenance}_${begin// /_}.db"
      "$DOLTLITE" "$DB" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-Am','init');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(10,'branch');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'working');
" >/dev/null 2>&1
      case "$maintenance" in
        none) stmt="INSERT INTO t VALUES(3,'peer');" ;;
        gc) stmt="INSERT INTO t VALUES(3,'peer'); SELECT dolt_gc();" ;;
        vacuum) stmt="INSERT INTO t VALUES(3,'peer'); VACUUM;" ;;
      esac
      if [ "$peer" = conn ]; then
        peer_write=".connection 1
.open $DB
$stmt
.connection 0"
      else
        peer_write=".shell $SHELL_DOLTLITE \"$DB\" \"$stmt\""
      fi
      rollback=""
      if [ "$begin" = BEGIN ]; then rollback="ROLLBACK;"; fi
      out=$("$DOLTLITE" "$DB" 2>"$ROOT/checkout.err" <<SQL
$begin;
SELECT count(*) FROM t;
$peer_write
SELECT dolt_checkout('b1');
$rollback
SELECT 'fresh', active_branch(), group_concat(id) FROM (SELECT id FROM t ORDER BY id);
SELECT dolt_checkout('b1');
SELECT dolt_checkout('b1');
INSERT INTO t VALUES(11,'after checkout');
SELECT 'target', active_branch(), group_concat(id) FROM (SELECT id FROM t ORDER BY id);
SELECT dolt_checkout('main');
SELECT 'peer', active_branch(), group_concat(id) FROM (SELECT id FROM t ORDER BY id);
PRAGMA integrity_check;
SQL
)
      result=$(printf '%s\n' "$out" | tr -d '\r' | awk '/^(fresh\||target\||peer\||ok$)/')
      expected=$'fresh|main|1,2,3\ntarget|b1|1,10,11\npeer|main|1,2,3\nok'
      label="checkout retry $peer $maintenance $begin"
      if [ "$result" = "$expected" ]; then
        dltest_pass "$label"
      else
        dltest_fail "$label" "  expected: $expected\n  got: $out\n  errors: $(cat "$ROOT/checkout.err")"
      fi
      run_test "checkout peer kept $peer $maintenance $begin" \
        "SELECT id FROM t ORDER BY id; PRAGMA integrity_check;" \
        $'1\n2\n3\nok' "$DB"
    done
  done
done

dltest_finish
