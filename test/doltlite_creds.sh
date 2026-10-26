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
KID=$("$DOLTLITE" "$DB" "SELECT kid FROM dolt_creds_ls() WHERE active;")
KID=${KID//$'\r'/}
run_test_match "creds_list_has_kid" "SELECT dolt_creds('list');" "$KID" "$DB"
run_test_match "creds_export_public_jwk" "SELECT dolt_creds('export', '$KID');" '"kty"' "$DB"
run_test "creds_export_no_private_key" "SELECT instr(dolt_creds('export', '$KID'), '\"d\"') = 0;" "1" "$DB"
run_test "creds_export_to_dir" "SELECT dolt_creds('export', '$KID', '$DOLTLITE_CREDS_DIR/authorized');" "Exported public credential $KID to $DOLTLITE_CREDS_DIR/authorized" "$DB"
if [ -n "$(ls "$DOLTLITE_CREDS_DIR/authorized" 2>/dev/null)" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: creds_export_dir_written"; fi
run_test "creds_ls_structured" "SELECT length(kid), length(public_key), active, error IS NULL FROM dolt_creds_ls();" "45|52|1|1" "$DB"
run_test "creds_second_new_preserves_active" "SELECT dolt_creds_new() IS NOT NULL; SELECT count(*), sum(active), max(CASE WHEN active THEN kid END) = '$KID' FROM dolt_creds_ls();" "1
2|1|1" "$DB"
OTHER=$("$DOLTLITE" "$DB" "SELECT kid FROM dolt_creds_ls() WHERE NOT active;")
OTHER=${OTHER//$'\r'/}
PUB=$("$DOLTLITE" "$DB" "SELECT public_key FROM dolt_creds_ls() WHERE kid='$OTHER';")
PUB=${PUB//$'\r'/}
run_test "creds_use_public_key" "SELECT dolt_creds_use('$PUB'); SELECT kid='$OTHER' FROM dolt_creds_ls() WHERE active=true;" "0
1" "$DB"
run_test "creds_use_survives_reopen" "SELECT kid='$OTHER' FROM dolt_creds_ls() WHERE active=true;" "1" "$DB"
run_test "creds_session_override" "SELECT dolt_creds_use('$KID','session'); SELECT kid='$KID' FROM dolt_creds_ls() WHERE active=true;" "0
1" "$DB"
run_test "creds_session_does_not_change_default" "SELECT kid='$OTHER' FROM dolt_creds_ls() WHERE active=true;" "1" "$DB"
export DOLTLITE_CREDS_KID="$KID"
run_test "creds_env_overrides_default" "SELECT kid='$KID' FROM dolt_creds_ls() WHERE active=true;" "1" "$DB"
run_test "creds_session_overrides_env" "SELECT dolt_creds_use('$OTHER','session'); SELECT kid='$OTHER' FROM dolt_creds_ls() WHERE active=true;" "0
1" "$DB"
unset DOLTLITE_CREDS_KID
run_test "creds_legacy_use_wrapper" "SELECT dolt_creds('use','$KID'); SELECT kid='$KID' FROM dolt_creds_ls() WHERE active=true;" "0
1" "$DB"
run_test "creds_named_export_omits_private" "SELECT instr(dolt_creds_export('$PUB'), '\"d\"')=0;" "1" "$DB"
run_test_error_match "creds_use_missing" "SELECT dolt_creds_use('missing');" "no such credential" "$DB"
run_test_error_match "creds_use_null" "SELECT dolt_creds_use(NULL);" "usage:" "$DB"
run_test_error_match "creds_new_args" "SELECT dolt_creds_new('extra');" "takes no arguments" "$DB"
run_test_error_match "creds_import_public_only" "SELECT dolt_creds_import('$DOLTLITE_CREDS_DIR/authorized/$KID.jwk');" "valid private JWK" "$DB"
run_test "creds_remove_by_public_key" "SELECT dolt_creds_rm('$PUB');" "0" "$DB"
run_test_match "creds_rm" "SELECT dolt_creds('rm', '$KID');" "Removed credential $KID" "$DB"
run_test_error_match "creds_rm_unknown" "SELECT dolt_creds('rm', '$KID');" "no such credential" "$DB"

run_test "creds_no_active_after_removal" "SELECT count(*) FROM dolt_creds_ls() WHERE active=true;" "0" "$DB"
rm -rf "$DOLTLITE_CREDS_DIR" "$DB" "$DB-lock"
dltest_finish
