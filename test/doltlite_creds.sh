#!/bin/bash
# Credential lifecycle through the SQL surface. Key generation and signing
# run the vendored Ed25519 code, so this is where the sanitizer job sees it.
DLTEST_STRIP_CR=1
. "$(dirname "$0")/lib/doltlite_test_common.sh"
echo "=== Doltlite credential tests ==="
echo ""
export DOLTLITE_CREDS_DIR=/tmp/test_creds_dir_$$; rm -rf "$DOLTLITE_CREDS_DIR"
DB=/tmp/test_creds_$$.db; rm -f "$DB" "$DB-lock"

run_test_match "creds_none_yet" "SELECT dolt_creds();" "no credentials; run SELECT dolt_creds_new" "$DB"
run_test_match "creds_new" "SELECT dolt_creds_new();" "Created credential [a-z0-9]+" "$DB"
KID=$(ls "$DOLTLITE_CREDS_DIR" | head -1 | sed 's/\.jwk$//')
run_test_match "creds_list_has_kid" "SELECT dolt_creds('list');" "$KID" "$DB"
run_test_match "creds_export_public_jwk" "SELECT dolt_creds('export', '$KID');" '"kty"' "$DB"
run_test "creds_export_no_private_key" "SELECT instr(dolt_creds('export', '$KID'), '\"d\"') = 0;" "1" "$DB"
run_test_match "creds_export_to_dir" "SELECT dolt_creds('export', '$KID', '$DOLTLITE_CREDS_DIR/authorized');" "" "$DB"
if [ -n "$(ls "$DOLTLITE_CREDS_DIR/authorized" 2>/dev/null)" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: creds_export_dir_written"; fi
run_test_match "creds_rm" "SELECT dolt_creds('rm', '$KID');" "Removed credential $KID" "$DB"
run_test_match "creds_rm_unknown" "SELECT dolt_creds('rm', '$KID');" "no such credential" "$DB"

rm -rf "$DOLTLITE_CREDS_DIR" "$DB" "$DB-lock"
dltest_finish
