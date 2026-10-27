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
run_test_error_match "creds_export_dir_refuses_overwrite" "SELECT dolt_creds_export('$KID', '$DOLTLITE_CREDS_DIR/authorized');" "failed to export public credential" "$DB"
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
CONFIG="$DOLTLITE_CREDS_DIR/config"
printf 'user.creds=%s\r\nfuture.setting=a=b' "$OTHER" > "$CONFIG"
run_test "creds_config_crlf_and_unknown_key" "SELECT kid='$OTHER' FROM dolt_creds_ls() WHERE active=true;" "1" "$DB"
run_test "creds_use_keeps_unknown_key" "SELECT dolt_creds_use('$KID');" "0" "$DB"
if grep -qx 'future.setting=a=b' "$CONFIG" && grep -qx "user.creds=$KID" "$CONFIG"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: creds_config_rewrite_preserves_unknown_key"; fi
printf 'garbage\nuser.creds=%s\n' "$OTHER" > "$CONFIG"
"$DOLTLITE" "$DB" "SELECT dolt_remote('add','cfg','https://127.0.0.1:1/x.db');" >/dev/null 2>&1
run_test_error_match "creds_bad_config_names_path" "SELECT dolt_fetch('cfg');" "cannot read credential configuration .*config; dolt_creds_use" "$DB"
run_test "creds_use_repairs_bad_config" "SELECT dolt_creds_use('$OTHER'); SELECT kid='$OTHER' FROM dolt_creds_ls() WHERE active=true;" "0
1" "$DB"
if ! grep -q garbage "$CONFIG"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: creds_use_drops_bad_line"; fi
"$DOLTLITE" "$DB" "SELECT dolt_remote('rm','cfg');" >/dev/null 2>&1
mv "$CONFIG" "$CONFIG.saved"; mkdir "$CONFIG"
NKEYS=$(ls "$DOLTLITE_CREDS_DIR"/*.jwk | wc -l)
run_test_error_match "creds_new_unsaved_selection" "SELECT dolt_creds_new();" "failed to save credential selection" "$DB"
if [ "$(ls "$DOLTLITE_CREDS_DIR"/*.jwk | wc -l)" = "$NKEYS" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: creds_new_unsaved_selection_leaves_no_key"; fi
rmdir "$CONFIG"; mv "$CONFIG.saved" "$CONFIG"
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
SRC=$(mktemp)
cp "$DOLTLITE_CREDS_DIR/$KID.jwk" "$SRC"
run_test "creds_reimport_private" "SELECT dolt_creds_import('$SRC');" "0" "$DB"
cp "$DOLTLITE_CREDS_DIR/$KID.jwk" "$SRC.kept"
# A zero file-size limit makes the replacement write fail. SIGXFSZ is ignored
# so write() returns EFBIG and the save can remove its temporary file.
IMPORT_RC=0
IMPORT_OUT=$(python3 -c '
import resource, signal, subprocess, sys
def pre():
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    resource.setrlimit(resource.RLIMIT_FSIZE, (0, 0))
r = subprocess.run([sys.argv[1], sys.argv[2], sys.argv[3]],
                   capture_output=True, text=True, preexec_fn=pre)
sys.stdout.write(r.stdout)
sys.stderr.write(r.stderr)
raise SystemExit(r.returncode)
' "$DOLTLITE" "$DB" "SELECT dolt_creds_import('$SRC');" 2>&1) || IMPORT_RC=$?
if [ "$IMPORT_RC" -ne 0 ] && printf '%s\n' "$IMPORT_OUT" | grep -F "failed to save imported credential" >/dev/null; then
  dltest_pass
else
  dltest_fail "creds_import_full_disk" "  rc=$IMPORT_RC\n  got: $IMPORT_OUT"
fi
if cmp -s "$SRC.kept" "$DOLTLITE_CREDS_DIR/$KID.jwk"; then
  dltest_pass
else
  dltest_fail "creds_import_full_disk_preserves_bytes" "  key file changed"
fi
rm -f "$SRC.kept"
run_test "creds_import_full_disk_still_readable" "SELECT error IS NULL AND public_key IS NOT NULL FROM dolt_creds_ls() WHERE kid='$KID';" "1" "$DB"
LEFT=$(find "$DOLTLITE_CREDS_DIR" \( -name '*.jwk.*' -o -name '*.tmp' \) -print | wc -l | tr -d '[:space:]')
dltest_assert_equal "creds_import_full_disk_no_temp" "$LEFT" "0"
rm -f "$SRC"
"$DOLTLITE" "$DB" "SELECT dolt_creds_new(); SELECT dolt_creds_new();" >/dev/null
EXTRA=$("$DOLTLITE" "$DB" "SELECT kid FROM dolt_creds_ls() WHERE kid NOT IN ('$KID','$OTHER') ORDER BY kid;")
EXTRA=${EXTRA//$'\r'/}
A=${EXTRA%%$'\n'*}
C=${EXTRA#*$'\n'}
run_test_error_match "creds_rm_unknown_deletes_nothing" "SELECT dolt_creds_rm('$A','nosuchkey','$C');" "no such credential" "$DB"
run_test "creds_rm_unknown_keeps_both" "SELECT count(*) FROM dolt_creds_ls() WHERE kid IN ('$A','$C');" "2" "$DB"
run_test "creds_rm_several" "SELECT dolt_creds_rm('$A','$C'); SELECT count(*) FROM dolt_creds_ls() WHERE kid IN ('$A','$C');" "0
0" "$DB"
"$DOLTLITE" "$DB" "SELECT dolt_creds_new();" >/dev/null
DUP=$("$DOLTLITE" "$DB" "SELECT kid FROM dolt_creds_ls() WHERE kid NOT IN ('$KID','$OTHER');")
DUP=${DUP//$'\r'/}
run_test "creds_rm_duplicate_name" "SELECT dolt_creds_rm('$DUP','$DUP'); SELECT count(*) FROM dolt_creds_ls() WHERE kid='$DUP';" "0
0" "$DB"
run_test "creds_remove_by_public_key" "SELECT dolt_creds_rm('$PUB');" "0" "$DB"
run_test_match "creds_rm" "SELECT dolt_creds('rm', '$KID');" "Removed credential $KID" "$DB"
run_test_error_match "creds_rm_unknown" "SELECT dolt_creds('rm', '$KID');" "no such credential" "$DB"

run_test "creds_no_active_after_removal" "SELECT count(*) FROM dolt_creds_ls() WHERE active=true;" "0" "$DB"
rm -rf "$DOLTLITE_CREDS_DIR" "$DB" "$DB-lock"
dltest_finish
