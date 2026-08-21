#!/bin/bash
# PK *values* (INT64 bounds, empty/long strings, binary, doubles) through commit/diff/AS OF/merge vs Dolt.

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

translate_for_dolt() {
  sed -E '
    s/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g
    s/dolt_diff_(stat|summary)([^a-zA-Z0-9_])/@@DOLT_DIFF_\1@@\2/g
    s/dolt_diff_([a-zA-Z0-9_]+)\(([^)]*)\)/dolt_diff(\2, "\1")/g
    s/@@DOLT_DIFF_(stat|summary)@@/dolt_diff_\1/g
  '
}

oracle() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^R|' | sort)

  local dolt_setup dolt_query
  dolt_setup=$(echo "$setup" | translate_for_dolt)
  dolt_query=$(echo "$query" | translate_for_dolt)

  local dt_out
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      echo "$dolt_setup"
      echo "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: PK edge values ==="
echo ""

echo "--- int64 boundaries ---"

INT_SETUP="
CREATE TABLE t(k BIGINT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (-9223372036854775808,'min'), (-1,'neg'), (0,'zero'),
                     (1,'one'), (9223372036854775807,'max');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
DELETE FROM t WHERE k = -1;
INSERT INTO t VALUES (-9223372036854775807,'min1');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
"

oracle "int64_diff" "$INT_SETUP" \
  "SELECT CONCAT('R|', IFNULL(to_k,''), '|', IFNULL(from_k,''), '|', diff_type) FROM dolt_diff_t('HEAD~1','HEAD');"

oracle "int64_history" "$INT_SETUP" \
  "SELECT CONCAT('R|', k, '=', v) FROM dolt_history_t WHERE commit_hash = (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1);"

oracle "int64_state" "$INT_SETUP" \
  "SELECT CONCAT('R|', k, '=', v) FROM t ORDER BY k;"

oracle "int64_merge" "
CREATE TABLE t(k BIGINT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (0,'zero');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (-9223372036854775808,'min'), (9223372036854775807,'max');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (-42,'neg');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feat');
" "SELECT CONCAT('R|', k, '=', v) FROM t ORDER BY k;"

echo "--- text edges: empty, unicode, 1KB keys ---"

LONGX=$(printf 'x%.0s' $(seq 1 1000))
TXT_SETUP="
CREATE TABLE t(k VARCHAR(1100) PRIMARY KEY, v TEXT);
INSERT INTO t VALUES ('','empty'), ('żółć','unicode'), ('ascii','plain');
INSERT INTO t VALUES ('long_${LONGX}', 'bigkey');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
DELETE FROM t WHERE k = 'żółć';
INSERT INTO t VALUES ('後で','unicode2');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
"

oracle "text_edge_diff" "$TXT_SETUP" \
  "SELECT CONCAT('R|', SUBSTR(HEX(IFNULL(to_k,'')),1,12), '|', OCTET_LENGTH(IFNULL(to_k,'')), '|', SUBSTR(HEX(IFNULL(from_k,'')),1,12), '|', diff_type) FROM dolt_diff_t('HEAD~1','HEAD');"

oracle "text_edge_state" "$TXT_SETUP" \
  "SELECT CONCAT('R|', SUBSTR(HEX(k),1,12), '|', OCTET_LENGTH(k), '=', v) FROM t;"

oracle "text_edge_history" "$TXT_SETUP" \
  "SELECT CONCAT('R|', SUBSTR(HEX(k),1,12), '|', OCTET_LENGTH(k)) FROM dolt_history_t;"

echo "--- binary keys (VARBINARY) ---"

BIN_SETUP="
CREATE TABLE t(k VARBINARY(32) PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (x'00','nul'), (x'DEADBEEF','deadbeef'), (x'FF00FF','stripes');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
DELETE FROM t WHERE k = x'DEADBEEF';
INSERT INTO t VALUES (x'0102030405060708090A0B0C0D0E0F10','sixteen');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
"

oracle "binary_diff" "$BIN_SETUP" \
  "SELECT CONCAT('R|', IFNULL(HEX(to_k),''), '|', IFNULL(HEX(from_k),''), '|', diff_type) FROM dolt_diff_t('HEAD~1','HEAD');"

oracle "binary_state" "$BIN_SETUP" \
  "SELECT CONCAT('R|', HEX(k), '=', v) FROM t ORDER BY k;"

oracle "binary_history" "$BIN_SETUP" \
  "SELECT CONCAT('R|', HEX(k)) FROM dolt_history_t WHERE commit_hash = (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1);"

echo "--- double keys (membership-based to dodge float formatting) ---"

DBL_SETUP="
CREATE TABLE t(k DOUBLE PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (3.25,'a'), (-1.5,'b'), (1e15,'big'), (0.001,'small');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
DELETE FROM t WHERE k = -1.5;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
"

oracle "double_membership" "$DBL_SETUP" \
  "SELECT CONCAT('R|', (SELECT count(*) FROM t), '|', (SELECT count(*) FROM t WHERE k = 3.25), '|', (SELECT count(*) FROM t WHERE k = 1e15), '|', (SELECT v FROM t WHERE k = 0.001));"

oracle "double_diff_count" "$DBL_SETUP" \
  "SELECT CONCAT('R|', diff_type, '|', count(*)) FROM dolt_diff_t('HEAD~1','HEAD') GROUP BY diff_type;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ -n "$FAILED_NAMES" ]; then
  echo "Failed:$FAILED_NAMES"
fi
[ "$fail" -eq 0 ]
