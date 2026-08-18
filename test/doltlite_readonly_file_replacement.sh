#!/bin/bash
# GC republishes the store by renaming a rebuilt file over the database path,
# and VACUUM and a WAL checkpoint both reach it. Neither may run for a
# connection that may not write, nor for one whose file was replaced
# underneath it -- that path now names a database this handle never opened,
# and the rename would destroy it.
DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
. "$(dirname "$0")/lib/doltlite_test_common.sh"

if [ "$(uname -s)" != "Darwin" ] && [ "$(uname -s)" != "Linux" ]; then
  echo "SKIP: file-replacement test is unix-only"
  exit 0
fi

echo "=== Doltlite read-only and replaced-file rejection ==="
echo ""

ROOT=$(mktemp -d /tmp/dl_ro_repl_XXXXXX)
trap 'chmod -R u+w "$ROOT" 2>/dev/null; rm -rf "$ROOT"' EXIT

seed_db() {
  rm -f "$1"
  echo "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);
INSERT INTO t VALUES(1,'kept');
SELECT dolt_commit('-A','-m','seed');" | $DOLTLITE "$1" > /dev/null 2>&1
}

# --- A read-only connection must not rewrite the file --------------------
for op in "VACUUM;" "PRAGMA wal_checkpoint;" "SELECT dolt_gc();"; do
  DB="$ROOT/ro.db"
  seed_db "$DB"
  ln "$DB" "$ROOT/link.db"
  chmod 0444 "$DB"
  before=$(stat -f "%z-%Lp-%i-%l" "$DB" 2>/dev/null || stat -c "%s-%a-%i-%h" "$DB")
  $DOLTLITE "file:$DB?mode=ro" "$op" > /dev/null 2>&1
  after=$(stat -f "%z-%Lp-%i-%l" "$DB" 2>/dev/null || stat -c "%s-%a-%i-%h" "$DB")
  label=$(echo "$op" | tr -cd '[:alnum:]_')
  if [ "$before" = "$after" ]; then
    dltest_pass "readonly_untouched_$label"
  else
    dltest_fail "readonly_untouched_$label" \
      "  file changed: $before -> $after (size-mode-inode-links)"
  fi
  chmod u+w "$DB"; rm -f "$DB" "$ROOT/link.db"
done

# Reads keep working on such a connection.
DB="$ROOT/ro.db"
seed_db "$DB"
chmod 0444 "$DB"
run_test "readonly_still_reads" "SELECT b FROM t WHERE a=1;" "kept" "file:$DB?mode=ro"
chmod u+w "$DB"; rm -f "$DB"

# --- A replaced file must not be overwritten -----------------------------
# The session reads once so the store has the file open, the path is then
# replaced by an unrelated database, and the operation must leave it alone.
for op in "VACUUM;" "PRAGMA wal_checkpoint;" "SELECT dolt_gc();"; do
  DB="$ROOT/live.db"
  seed_db "$DB"
  rm -f "$ROOT/victim.db"
  echo "CREATE TABLE victim(x TEXT);
INSERT INTO victim VALUES('untouched');
SELECT dolt_commit('-A','-m','victim');" | $DOLTLITE "$ROOT/victim.db" > /dev/null 2>&1
  printf 'SELECT count(*) FROM t;\n.shell mv %s %s\n%s\n' \
    "$ROOT/victim.db" "$DB" "$op" | $DOLTLITE "$DB" > /dev/null 2>&1
  label=$(echo "$op" | tr -cd '[:alnum:]_')
  run_test "replaced_survives_$label" "SELECT x FROM victim;" "untouched" "$DB"
  rm -f "$DB"
done

# Surviving is not enough: the caller is working on a path that no longer
# holds the database it opened, and only the refusal tells it so. A merely
# read-only store stays silent instead, because stock answers that with
# success and compacting on checkpoint is opportunistic either way.
DB="$ROOT/live.db"
seed_db "$DB"
rm -f "$ROOT/victim.db"
echo "CREATE TABLE victim(x TEXT);
INSERT INTO victim VALUES('untouched');
SELECT dolt_commit('-A','-m','victim');" | $DOLTLITE "$ROOT/victim.db" > /dev/null 2>&1
out=$(printf 'SELECT count(*) FROM t;\n.shell mv %s %s\nPRAGMA wal_checkpoint;\n' \
        "$ROOT/victim.db" "$DB" | $DOLTLITE "$DB" 2>&1 | tail -1)
case "$out" in
  *readonly*) dltest_pass "replaced_checkpoint_reports" ;;
  *) dltest_fail "replaced_checkpoint_reports" \
       "  expected a readonly refusal\n  got:      $out" ;;
esac
rm -f "$DB"

DB="$ROOT/ro2.db"
seed_db "$DB"
chmod 0444 "$DB"
run_test_match "readonly_checkpoint_stays_quiet" "PRAGMA wal_checkpoint;" \
  "^[0-9]+\|" "file:$DB?mode=ro"
chmod u+w "$DB"; rm -f "$DB"

dltest_finish
