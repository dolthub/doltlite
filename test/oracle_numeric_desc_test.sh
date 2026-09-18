#!/bin/bash
set -uo pipefail
DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/sql_oracle_common.sh"
if ! sql_oracle_check_binaries "$SCRIPT_DIR"; then exit 1; fi
SQL_ORACLE_TMP=$(mktemp -d) || exit 1
trap 'rm -rf "$SQL_ORACLE_TMP"' EXIT
pass=0; fail=0

for numeric_order in ASC DESC; do
  for neighbor_order in ASC DESC; do
    for shape in index unique without_rowid; do
      for neighbor in "NULL" "17" "'x'" "x'00ff'"; do
        if [ "$shape" = without_rowid ] && [ "$neighbor" = NULL ]; then continue; fi
        schema="CREATE TABLE t(id INTEGER PRIMARY KEY, a, b); CREATE INDEX ti ON t(a $numeric_order,b $neighbor_order);"
        scan="t INDEXED BY ti"
        if [ "$shape" = unique ]; then
          schema="CREATE TABLE t(id INTEGER PRIMARY KEY, a, b); CREATE UNIQUE INDEX ti ON t(a $numeric_order,b $neighbor_order);"
        elif [ "$shape" = without_rowid ]; then
          schema="CREATE TABLE t(id INTEGER, a, b, PRIMARY KEY(a $numeric_order,b $neighbor_order)) WITHOUT ROWID;"
          scan=t
        fi
        setup="$schema"
        id=0
        for value in -9223372036854775808 -9223372036854775807 -9223372036854775806 -9007199254740995 -9007199254740994 -9007199254740993 -9007199254740992 -1 0 1 1.5 2 3 9007199254740991 9007199254740992 9007199254740993 9007199254740994 9007199254740995 9223372036854775806 9223372036854775807; do
          id=$((id+1))
          setup="$setup INSERT INTO t VALUES($id,$value,$neighbor);"
        done
        queries="SELECT a FROM $scan ORDER BY a; SELECT a FROM $scan ORDER BY a DESC;"
        for bound in -9223372036854775807 -9007199254740994 1 3 9007199254740992 9007199254740993 9223372036854775807; do
          for op in '<' '<=' '=' '>=' '>'; do
            queries="$queries SELECT a FROM $scan WHERE a $op $bound ORDER BY a;
              SELECT count(*) FROM $scan WHERE a $op $bound;"
          done
          queries="$queries SELECT count(*) FROM $scan WHERE a BETWEEN $bound AND $bound;
            SELECT count(*) FROM $scan WHERE a BETWEEN -9223372036854775808 AND $bound;"
        done
        oracle "${numeric_order}_${neighbor_order}_${shape}_${neighbor}" "$setup $queries
          UPDATE t SET b=$neighbor WHERE id=16;
          DELETE FROM t WHERE id=3;
          $queries PRAGMA integrity_check;"
      done
    done
  done
done

for shape in index without_rowid; do
  schema="CREATE TABLE t(id INTEGER PRIMARY KEY, a, b); CREATE INDEX ti ON t(a,b DESC);"
  scan="t INDEXED BY ti"
  if [ "$shape" = without_rowid ]; then
    schema="CREATE TABLE t(id INTEGER, a, b, PRIMARY KEY(a,b DESC)) WITHOUT ROWID;"
    scan=t
  fi
  oracle "deep_$shape" "$schema
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<60000)
    INSERT INTO t SELECT x,9007199254740990+x%13,x FROM c;
    SELECT count(*) FROM $scan WHERE a BETWEEN 9007199254740991 AND 9007199254740993;
    SELECT count(*) FROM $scan WHERE a BETWEEN 9007199254740992 AND 9007199254740992;
    SELECT a,count(*) FROM $scan GROUP BY a ORDER BY a;
    PRAGMA integrity_check;"
done
sql_oracle_finish
