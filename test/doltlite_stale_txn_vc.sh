#!/bin/bash
# A VC command inside a transaction that has only read must not overwrite a
# peer write committed after the snapshot.
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== VC commands in a stale read transaction keep peer writes ==="
echo ""

ROOT=$(mktemp -d /tmp/dl_stale_txn_vc_XXXXXX)
trap 'rm -rf "$ROOT"' EXIT

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

dltest_finish
