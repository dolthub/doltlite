#!/bin/bash
#
# Oracle tests for clustered-key VC operations at multi-chunk scale. The
# pk-shape suites use 3-5 row tables -- a single prolly leaf -- so tree
# splits, internal nodes, and chunk-boundary keys never form. Build ~1500-row
# tables per key shape, run branch edits over disjoint ranges plus a merge,
# and compare aggregates, boundary samples, and diff counts against Dolt.

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

echo "=== Version Control Oracle Tests: clustered keys at multi-chunk scale ==="
echo ""

GEN="WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 1500) SELECT"

for shape in comp_int text_pk pk_only; do
  case $shape in
    comp_int)
      DDL="CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));"
      SEED="INSERT INTO t $GEN FLOOR(n/40), n, CONCAT('v', n) FROM seq;"
      FEAT_EDIT="UPDATE t SET v = CONCAT(v, '_f') WHERE b >= 100 AND b < 200;
INSERT INTO t VALUES (99, 5000, 'featnew');"
      MAIN_EDIT="DELETE FROM t WHERE b >= 700 AND b < 760;
UPDATE t SET v = CONCAT(v, '_m') WHERE b >= 1400;"
      AGG="SELECT CONCAT('R|', count(*), '|', SUM(a), '|', SUM(b), '|', SUM(OCTET_LENGTH(v)), '|', MIN(b), '|', MAX(b)) FROM t;"
      SAMPLE="SELECT CONCAT('R|', a, '|', b, '|', v) FROM t ORDER BY a, b LIMIT 12 OFFSET 730;"
      ;;
    text_pk)
      DDL="CREATE TABLE t(k VARCHAR(40), v TEXT, PRIMARY KEY(k));"
      SEED="INSERT INTO t $GEN CONCAT('key_', 100000 + n), CONCAT('v', n) FROM seq;"
      FEAT_EDIT="UPDATE t SET v = CONCAT(v, '_f') WHERE k >= 'key_100100' AND k < 'key_100200';
INSERT INTO t VALUES ('key_zzz', 'featnew');"
      MAIN_EDIT="DELETE FROM t WHERE k >= 'key_100700' AND k < 'key_100760';
UPDATE t SET v = CONCAT(v, '_m') WHERE k >= 'key_101400';"
      AGG="SELECT CONCAT('R|', count(*), '|', SUM(OCTET_LENGTH(k)), '|', SUM(OCTET_LENGTH(v)), '|', MIN(k), '|', MAX(k)) FROM t;"
      SAMPLE="SELECT CONCAT('R|', k, '|', v) FROM t ORDER BY k LIMIT 12 OFFSET 730;"
      ;;
    pk_only)
      DDL="CREATE TABLE t(p INTEGER, c INTEGER, PRIMARY KEY(p, c));"
      SEED="INSERT INTO t $GEN FLOOR(n/40), n FROM seq;"
      FEAT_EDIT="INSERT INTO t $GEN 999, n + 5000 FROM seq WHERE n <= 80;"
      MAIN_EDIT="DELETE FROM t WHERE c >= 700 AND c < 760;"
      AGG="SELECT CONCAT('R|', count(*), '|', SUM(p), '|', SUM(c), '|', MIN(c), '|', MAX(c)) FROM t;"
      SAMPLE="SELECT CONCAT('R|', p, '|', c) FROM t ORDER BY p, c LIMIT 12 OFFSET 730;"
      ;;
  esac

  echo "--- shape: $shape (1500 rows) ---"

  SCALE_SETUP="
$DDL
$SEED
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
SELECT dolt_checkout('-b', 'feat');
$FEAT_EDIT
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
$MAIN_EDIT
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feat');
"

  # Ranged diff on the linear pre-merge history: post-merge per-commit
  # attribution differs between the systems by design.
  LINEAR_SETUP="
$DDL
$SEED
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
$MAIN_EDIT
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main2');
"

  oracle "${shape}_scale_agg" "$SCALE_SETUP" "$AGG"
  oracle "${shape}_scale_sample" "$SCALE_SETUP" "$SAMPLE"
  oracle "${shape}_scale_diff_counts" "$LINEAR_SETUP" \
    "SELECT CONCAT('R|', diff_type, '|', count(*)) FROM dolt_diff_t('HEAD~1','HEAD') GROUP BY diff_type;"
  oracle "${shape}_scale_history_count" "$SCALE_SETUP" \
    "SELECT CONCAT('R|', count(*)) FROM dolt_history_t;"
done

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ -n "$FAILED_NAMES" ]; then
  echo "Failed:$FAILED_NAMES"
fi
[ "$fail" -eq 0 ]
