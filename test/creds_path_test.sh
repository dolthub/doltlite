#!/bin/bash
set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/creds" "$TMP/authkeys"
pass=0
fail=0

check() {
  local name="$1"
  shift
  if "$@"; then
    echo "  PASS  $name"
    pass=$((pass + 1))
  else
    echo "  FAIL  $name"
    fail=$((fail + 1))
  fi
}

reject_remove() {
  local kid="$1"
  ! DOLTLITE_CREDS_DIR="$TMP/creds" "$DOLTLITE" :memory: \
    "SELECT dolt_creds('rm','$kid');" >/dev/null 2>&1
}

has_no_private_key() {
  ! grep -q '"d"' "$1"
}

reject_export_to_private() {
  ! DOLTLITE_CREDS_DIR="$TMP/creds" "$DOLTLITE" :memory: \
    "SELECT dolt_creds('export','$kid','$TMP/creds');" >/dev/null 2>&1
}

echo "=== doltlite credential path tests ==="
printf '{}' > "$TMP/victim.jwk"
check "parent traversal removal rejected" reject_remove "../victim"
check "outside file preserved" test -f "$TMP/victim.jwk"
check "Windows separator traversal rejected" reject_remove '..\victim'
check "Windows drive-relative KID rejected" reject_remove 'C:victim'
check "absolute KID rejected" reject_remove '/tmp/victim'

created=$(DOLTLITE_CREDS_DIR="$TMP/creds" "$DOLTLITE" :memory: \
  "SELECT dolt_creds_new();" 2>/dev/null)
created=${created//$'\r'/}
kid=$(printf '%s\n' "$created" | sed -n \
  's/^Created credential \([0-9a-v][0-9a-v]*\)$/\1/p' | head -1)
check "canonical credential created" test "${#kid}" -eq 45
check "canonical credential stored" test -f "$TMP/creds/$kid.jwk"
public=$(DOLTLITE_CREDS_DIR="$TMP/creds" "$DOLTLITE" :memory: \
  "SELECT dolt_creds('export','$kid');" 2>/dev/null)
check "public export contains public key" grep -q '"x"' <<<"$public"
if [[ "$public" != *'"d"'* ]]; then
  check "public export omits private seed" true
else
  check "public export omits private seed" false
fi
check "public credential exports to authorization directory" env \
  DOLTLITE_CREDS_DIR="$TMP/creds" "$DOLTLITE" :memory: \
  "SELECT dolt_creds('export','$kid','$TMP/authkeys');"
check "authorization file created" test -f "$TMP/authkeys/$kid.jwk"
check "authorization file omits private seed" has_no_private_key \
  "$TMP/authkeys/$kid.jwk"
check "public export cannot overwrite private credential" \
  reject_export_to_private
check "private credential survives refused export" grep -q '"d"' \
  "$TMP/creds/$kid.jwk"
check "canonical credential removal succeeds" env \
  DOLTLITE_CREDS_DIR="$TMP/creds" "$DOLTLITE" :memory: \
  "SELECT dolt_creds('rm','$kid');"
check "canonical credential removed" test ! -e "$TMP/creds/$kid.jwk"

echo ""
echo "Results: $pass passed, $fail failed"
test "$fail" -eq 0
