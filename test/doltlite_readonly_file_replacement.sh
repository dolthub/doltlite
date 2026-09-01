#!/bin/bash
# GC/VACUUM/checkpoint rename over the db path; refuse if the handle may not write or the file was replaced.
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

# GNU stat spells the format -c and uses -f for filesystem info; BSD stat is
# the reverse. Probing -c first keeps the Linux path from capturing statfs
# output, whose free-block counters any concurrent write perturbs -- the file
# signature then compares unequal even though the file never changed.
file_signature() {
  stat -c "%s-%a-%i-%h" "$1" 2>/dev/null || stat -f "%z-%Lp-%i-%l" "$1"
}

seed_db() {
  rm -f "$1"
  echo "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);
INSERT INTO t VALUES(1,'kept');
SELECT dolt_commit('-A','-m','seed');" | $DOLTLITE "$1" > /dev/null 2>&1
}

for op in "VACUUM;" "PRAGMA wal_checkpoint;" "SELECT dolt_gc();"; do
  DB="$ROOT/ro.db"
  seed_db "$DB"
  ln "$DB" "$ROOT/link.db"
  chmod 0444 "$DB"
  before=$(file_signature "$DB")
  $DOLTLITE "file:$DB?mode=ro" "$op" > /dev/null 2>&1
  after=$(file_signature "$DB")
  label=$(echo "$op" | tr -cd '[:alnum:]_')
  if [ "$before" = "$after" ]; then
    dltest_pass "readonly_untouched_$label"
  else
    dltest_fail "readonly_untouched_$label" \
      "  file changed: $before -> $after (size-mode-inode-links)"
  fi
  chmod u+w "$DB"; rm -f "$DB" "$ROOT/link.db"
done

DB="$ROOT/ro.db"
seed_db "$DB"
chmod 0444 "$DB"
run_test "readonly_still_reads" "SELECT b FROM t WHERE a=1;" "kept" "file:$DB?mode=ro"
chmod u+w "$DB"; rm -f "$DB"

# Session has the file open; replacing the path with another database must leave it alone.
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

# Must refuse, not silently succeed: a merely read-only store stays silent (stock compact is opportunistic).
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
