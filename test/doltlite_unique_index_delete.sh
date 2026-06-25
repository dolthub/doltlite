#!/bin/bash

source "$(dirname "$0")/lib/doltlite_test_common.sh"

db_rm() {
  rm -rf "$1" "${1}-wal"
}

echo "=== UNIQUE secondary index delete ==="
echo ""

DB=/tmp/test_unique_index_delete_$$.db; db_rm "$DB"

run_test "blob_pk_unique_index_delete_index_scan" \
  "CREATE TABLE t(id BLOB PRIMARY KEY NOT NULL, a BLOB NOT NULL, b BLOB NOT NULL, c INTEGER NOT NULL) STRICT;
   CREATE UNIQUE INDEX t_uidx ON t(a, b, c);
   INSERT INTO t VALUES (x'01', x'70', x'65', 0);
   DELETE FROM t WHERE id=x'01';
   SELECT changes();
   SELECT count(*) FROM t INDEXED BY t_uidx;" \
  "1
0" \
  "$DB"

db_rm "$DB"

run_test "blob_pk_unique_index_update_removes_old_entry" \
  "CREATE TABLE t(id BLOB PRIMARY KEY NOT NULL, a BLOB NOT NULL, b BLOB NOT NULL, c INTEGER NOT NULL) STRICT;
   CREATE UNIQUE INDEX t_uidx ON t(a, b, c);
   INSERT INTO t VALUES (x'01', x'70', x'65', 0);
   UPDATE t SET a=x'71' WHERE id=x'01';
   SELECT changes();
   SELECT count(*) FROM t INDEXED BY t_uidx WHERE a=x'70';
   SELECT count(*) FROM t INDEXED BY t_uidx WHERE a=x'71';" \
  "1
0
1" \
  "$DB"

db_rm "$DB"

run_test "blob_pk_unique_index_driven_delete" \
  "CREATE TABLE t(id BLOB PRIMARY KEY NOT NULL, a BLOB NOT NULL, b BLOB NOT NULL, c INTEGER NOT NULL) STRICT;
   CREATE UNIQUE INDEX t_uidx ON t(a, b, c);
   INSERT INTO t VALUES (x'01', x'70', x'65', 0), (x'02', x'70', x'66', 0);
   DELETE FROM t WHERE a=x'70';
   SELECT changes();
   SELECT count(*) FROM t;
   SELECT count(*) FROM t INDEXED BY t_uidx;" \
  "2
0
0" \
  "$DB"

db_rm "$DB"

run_test "blob_pk_fk_unique_index_delete_index_scan" \
  "PRAGMA foreign_keys=ON;
   CREATE TABLE edges (id BLOB PRIMARY KEY NOT NULL) STRICT;
   CREATE TABLE paths (id BLOB PRIMARY KEY NOT NULL) STRICT;
   CREATE TABLE path_edges (
     id BLOB PRIMARY KEY NOT NULL,
     path_id BLOB NOT NULL,
     edge_id BLOB NOT NULL,
     index_in_path INTEGER NOT NULL,
     FOREIGN KEY(edge_id) REFERENCES edges(id),
     FOREIGN KEY(path_id) REFERENCES paths(id)
   ) STRICT;
   CREATE UNIQUE INDEX path_edges_uidx ON path_edges(path_id, edge_id, index_in_path);
   INSERT INTO edges VALUES (x'65');
   INSERT INTO paths VALUES (x'70');
   INSERT INTO path_edges VALUES (x'01', x'70', x'65', 0);
   DELETE FROM path_edges WHERE id=x'01';
   SELECT changes();
   SELECT count(*) FROM path_edges INDEXED BY path_edges_uidx;" \
  "1
0" \
  "$DB"

db_rm "$DB"

dltest_finish
