#!/bin/bash
# Two attachments of the same named in-memory database in one connection see one store.
DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== named in-memory databases are shared ==="
echo ""

DB=":memory:"

run_test "shared_attach_twice_sees_one_store" "
ATTACH 'file:dlsh_a?mode=memory&cache=shared' AS a;
ATTACH 'file:dlsh_a?mode=memory&cache=shared' AS b;
CREATE TABLE a.t(id INT PRIMARY KEY, v TEXT);
INSERT INTO a.t VALUES(1,'a');
SELECT id, v FROM b.t;
INSERT INTO b.t VALUES(2,'b');
SELECT count(*) FROM a.t;
" "1|a
2" "$DB"

run_test "memdb_vfs_slash_name_shared" "
ATTACH 'file:/dlsh_b?vfs=memdb' AS a;
ATTACH 'file:/dlsh_b?vfs=memdb' AS b;
CREATE TABLE a.t(id INT PRIMARY KEY);
INSERT INTO a.t VALUES(5);
SELECT id FROM b.t;
" "5" "$DB"

run_test_match "private_mode_memory_not_shared" "
ATTACH 'file:dlsh_c?mode=memory' AS a;
ATTACH 'file:dlsh_c?mode=memory' AS b;
CREATE TABLE a.t(id INT PRIMARY KEY);
SELECT count(*) FROM b.t;
" "no such table: b.t" "$DB"

dltest_finish
