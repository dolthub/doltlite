#!/bin/bash
DLTEST_TIMEOUT=60
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== GC over shared commit history ==="
TASK_TMP=$(mktemp -d)
trap 'rm -rf "$TASK_TMP"' EXIT
DB="$TASK_TMP/base.db"
SQL="$TASK_TMP/create.sql"
cat > "$SQL" <<'SQL'
.bail on
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
INSERT INTO t SELECT value, 0 FROM generate_series(1,250000);
SELECT dolt_commit('-Am','seed');
SELECT dolt_branch('seed');
SQL
for ((i=0; i<240; i++)); do
  cat >> "$SQL" <<'SQL'
UPDATE t SET v=v+1 WHERE id IN (SELECT value FROM generate_series(10000,250000,10000));
SELECT dolt_commit('-Am','change');
SQL
done
if ! perl -e 'alarm(60);exec @ARGV' "$DOLTLITE" "$DB" < "$SQL" > "$TASK_TMP/setup.log" 2>&1; then
  dltest_fail history_setup "$(cat "$TASK_TMP/setup.log")"
  dltest_finish
fi

for mode in vacuum gc into; do
  SOURCE="$TASK_TMP/$mode.db"
  cp "$DB" "$SOURCE"
  case "$mode" in
    vacuum) STMT="VACUUM;" ;;
    gc) STMT="SELECT dolt_gc();" ;;
    into) STMT="VACUUM INTO '$TASK_TMP/copy.db';" ;;
  esac
  if dltest_require "history_${mode}_16mib" "$SOURCE" \
      "PRAGMA hard_heap_limit=16777216; $STMT"; then
    dltest_pass
  fi
  if [ "$mode" = into ]; then SOURCE="$TASK_TMP/copy.db"; fi
  run_test "history_${mode}_rows" \
    "SELECT count(*),sum(v) FROM t; SELECT count(*) FROM dolt_log;" \
    $'250000|6000\n242' "$SOURCE"
  if dltest_require "history_${mode}_checkout" "$SOURCE" \
      "SELECT dolt_checkout('seed');"; then
    run_test "history_${mode}_seed" \
      "SELECT count(*),sum(v) FROM t; PRAGMA integrity_check;" \
      $'250000|0\nok' "$SOURCE/seed"
  fi
done

dltest_finish
