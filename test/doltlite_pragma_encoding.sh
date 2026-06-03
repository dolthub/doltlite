#!/bin/bash
# Doltlite's prolly storage stores SQL text in UTF-8. Ignore unsupported
# database encodings instead of allowing SQLite's UTF-16 setup path to fail
# later while initializing the schema.

source "$(dirname "$0")/lib/doltlite_test_common.sh"

db_rm() {
  rm -rf "$1" "${1}-wal"
}

echo "=== PRAGMA encoding (doltlite-format) ==="
echo ""

DB=/tmp/test_encoding_dl_$$.db; db_rm "$DB"

run_test "encoding_default_utf8" \
  "PRAGMA encoding;" \
  "UTF-8" \
  "$DB"

run_test "encoding_utf8_ok" \
  "PRAGMA encoding='UTF-8'; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1, 'abc'); SELECT v FROM t;" \
  "abc" \
  "$DB"

db_rm "$DB"

run_test "encoding_utf16le_ignored" \
  "PRAGMA encoding='UTF-16le';" \
  "" \
  "$DB"

run_test "encoding_after_ignored_still_utf8" \
  "PRAGMA encoding;" \
  "UTF-8" \
  "$DB"

run_test "encoding_after_ignored_can_create" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1, 'abc'); PRAGMA encoding; SELECT v FROM t;" \
  "UTF-8
abc" \
  "$DB"

db_rm "$DB"

dltest_finish
