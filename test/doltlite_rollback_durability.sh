#!/bin/bash
DOLTLITE="${1:-./doltlite}"
TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT
PASS=0; FAIL=0; ERRORS=""

file_size() {
  case "$(uname -s)" in
    Darwin|FreeBSD) stat -f %z "$1" ;;
    *) stat -c %s "$1" ;;
  esac
}
file_sha()  { shasum -a 256 "$1" | awk '{print $1}'; }

stable_rollback() {
  local name="$1" setup="$2" txn="$3"
  local db="$TMP/${name}.db"
  rm -f "$db"
  if [ -n "$setup" ]; then
    printf '%s\n' "$setup" | "$DOLTLITE" "$db" >/dev/null 2>&1
  fi
  local size_before sha_before size_after sha_after
  size_before=$(file_size "$db")
  sha_before=$(file_sha "$db")
  printf '%s\n' "$txn" | "$DOLTLITE" "$db" >/dev/null 2>&1
  size_after=$(file_size "$db")
  sha_after=$(file_sha "$db")
  if [ "$size_before" = "$size_after" ] && [ "$sha_before" = "$sha_after" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  expected: file unchanged (size=$size_before sha=${sha_before:0:8})\n  got:      size=$size_after sha=${sha_after:0:8} (delta=$((size_after-size_before)))"
  fi
}

echo "=== ROLLBACK durability tests ==="
echo ""

SEED_T="CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','seed');"
SEED_PLAIN="CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','seed');"

stable_rollback "empty_begin_rollback" \
  "$SEED_PLAIN" "BEGIN; ROLLBACK;"

stable_rollback "dml_insert_rollback" \
  "$SEED_T" "BEGIN; INSERT INTO t VALUES(2,'b'); ROLLBACK;"

stable_rollback "dml_update_rollback" \
  "$SEED_T" "BEGIN; UPDATE t SET v='X' WHERE id=1; ROLLBACK;"

stable_rollback "dml_delete_rollback" \
  "$SEED_T" "BEGIN; DELETE FROM t WHERE id=1; ROLLBACK;"

stable_rollback "ddl_create_rollback" \
  "$SEED_PLAIN" "BEGIN; CREATE TABLE x(id INTEGER PRIMARY KEY); ROLLBACK;"

stable_rollback "ddl_drop_rollback" \
  "$SEED_T" "BEGIN; DROP TABLE t; ROLLBACK;"

stable_rollback "ddl_alter_rollback" \
  "$SEED_T" "BEGIN; ALTER TABLE t ADD COLUMN w INTEGER; ROLLBACK;"

stable_rollback "ddl_dml_rollback" \
  "$SEED_PLAIN" "BEGIN; CREATE TABLE x(id INTEGER PRIMARY KEY); INSERT INTO x VALUES(1); ROLLBACK;"

stable_rollback "savepoint_rollback_outer_rollback" \
  "$SEED_T" "BEGIN; INSERT INTO t VALUES(2,'b'); SAVEPOINT s1; INSERT INTO t VALUES(3,'c'); ROLLBACK TO s1; ROLLBACK;"

stable_rollback "nested_savepoints_outer_rollback" \
  "$SEED_T" "BEGIN; SAVEPOINT a; INSERT INTO t VALUES(2,'b'); SAVEPOINT b; INSERT INTO t VALUES(3,'c'); ROLLBACK TO a; ROLLBACK;"

stable_rollback "release_savepoint_then_outer_rollback" \
  "$SEED_T" "BEGIN; SAVEPOINT a; INSERT INTO t VALUES(2,'b'); RELEASE a; ROLLBACK;"

stable_rollback "trigger_rollback" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT); CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO log(msg) VALUES('inserted ' || NEW.id); END; SELECT dolt_commit('-A','-m','seed');" \
  "BEGIN; INSERT INTO t VALUES(1,'a'); ROLLBACK;"

stable_rollback "many_inserts_rollback" \
  "$SEED_T" \
  "BEGIN; INSERT INTO t SELECT n+10, hex(randomblob(8)) FROM (WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<100) SELECT n FROM seq); ROLLBACK;"

stable_rollback "rollback_after_select_only_inside_txn" \
  "$SEED_T" "BEGIN; SELECT count(*) FROM t; ROLLBACK;"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
