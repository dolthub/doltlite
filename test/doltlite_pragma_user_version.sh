#!/bin/bash

source "$(dirname "$0")/lib/doltlite_test_common.sh"

db_rm() {
  rm -rf "$1" "${1}-wal"
}

echo "=== PRAGMA user_version/application_id persistence ==="
echo ""

DB=/tmp/test_user_version_$$.db; db_rm "$DB"

run_test "user_version_default" \
  "PRAGMA user_version;" \
  "0" \
  "$DB"

run_test "user_version_same_connection" \
  "PRAGMA user_version=7; PRAGMA user_version;" \
  "7" \
  "$DB"

run_test "user_version_reopen" \
  "PRAGMA user_version;" \
  "7" \
  "$DB"

run_test "user_version_with_deferred_txn" \
  "BEGIN DEFERRED; CREATE TABLE t(id INTEGER PRIMARY KEY); PRAGMA user_version=11; COMMIT; PRAGMA user_version;" \
  "11" \
  "$DB"

run_test "user_version_deferred_txn_reopen" \
  "PRAGMA user_version; SELECT name FROM sqlite_master WHERE type='table' AND name='t';" \
  "11
t" \
  "$DB"

run_test "user_version_rollback" \
  "BEGIN; PRAGMA user_version=12; ROLLBACK; PRAGMA user_version;" \
  "11" \
  "$DB"

run_test "user_version_rollback_reopen" \
  "PRAGMA user_version;" \
  "11" \
  "$DB"

run_test "application_id_reopen" \
  "PRAGMA application_id=123456; PRAGMA application_id;" \
  "123456" \
  "$DB"

run_test "application_id_reopen_again" \
  "PRAGMA application_id;" \
  "123456" \
  "$DB"

db_rm "$DB"

dltest_finish
