#!/bin/bash
# A chunk the database's own refs point to is gone: users must see corruption,
# never the store's internal "unknown operation" sentinel, and the lazy-clone
# hint must not stand in for the missing chunk.
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Missing chunks report corruption ==="
echo ""

ROOT=$(mktemp -d /tmp/dl_missing_chunk_XXXXXX)
trap 'rm -rf "$ROOT"' EXIT
dltest_expect_integrity "$ROOT/ws.db" 'skip:fixture deliberately missing a chunk'
dltest_expect_integrity "$ROOT/open.db" 'skip:fixture deliberately missing a chunk'
DIR=$(cd "$(dirname "$0")" && pwd)

# Working set whose staged catalog chunk is missing.
fresh_ws() {
  rm -f "$ROOT/ws.db" "$ROOT/.ws.db-lock"
  cp "$DIR/missing_chunk_working_set_fixture.db" "$ROOT/ws.db"
}

expect_corrupt() {
  local name="$1" sql="$2" out
  out=$($DOLTLITE "$ROOT/ws.db" "$sql" 2>&1)
  case "$out" in
    *"unknown operation"*) dltest_fail "$name" "  leaked NOTFOUND: $out" ;;
    *malformed*|*"chunk is missing"*|*"failed to load staged catalog"*)
      dltest_pass "$name" ;;
    *) dltest_fail "$name" "  got: $out" ;;
  esac
}

fresh_ws
expect_corrupt "reset_hard_reports_corruption" "SELECT dolt_reset('--hard');"
fresh_ws
expect_corrupt "status_reports_corruption" "SELECT * FROM dolt_status;"

fresh_ws
$DOLTLITE "$ROOT/ws.db" \
  "SELECT dolt_remote('add','origin','file:///nonexistent/origin.db');" \
  >/dev/null 2>&1
out=$($DOLTLITE "$ROOT/ws.db" "SELECT dolt_reset('--hard');" 2>&1)
case "$out" in
  *"chunk is missing"*) dltest_pass "remote_configured_names_missing_chunk" ;;
  *) dltest_fail "remote_configured_names_missing_chunk" "  got: $out" ;;
esac
case "$out" in
  *"reopen with lazy_origin=1 for"*|*"unknown operation"*)
    dltest_fail "remote_configured_no_lazy_misdirection" "  got: $out" ;;
  *) dltest_pass "remote_configured_no_lazy_misdirection" ;;
esac

# A refs root whose working set chunk is missing at open.
rm -f "$ROOT/open.db" "$ROOT/.open.db-lock"
cp "$DIR/missing_chunk_open_fixture.db" "$ROOT/open.db"
out=$($DOLTLITE "$ROOT/open.db" "SELECT count(*) FROM t;" 2>&1)
case "$out" in
  *"unknown operation"*) dltest_fail "open_reports_corruption" "  got: $out" ;;
  *malformed*) dltest_pass "open_reports_corruption" ;;
  *) dltest_fail "open_reports_corruption" "  got: $out" ;;
esac

dltest_finish
