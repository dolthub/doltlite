#!/usr/bin/env bash
# Startup security warnings for doltlite-remotesrv. The two exposures of a
# non-loopback bind are independent: TLS covers confidentiality, --auth-keys
# covers access control, and an unauthenticated write request opens the store
# READWRITE|CREATE, so a reachable client can push as well as read. A single
# warning gated on "no TLS" left the TLS-without-auth case silent and told
# operators who had configured --auth-keys that they were unauthenticated.
set -uo pipefail

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
REMOTESRV="${2:-$(dirname "$0")/../build/doltlite-remotesrv}"

if [ ! -x "$DOLTLITE" ] || [ ! -x "$REMOTESRV" ]; then
  echo "SKIP: doltlite/doltlite-remotesrv binaries not found ($DOLTLITE, $REMOTESRV)"
  exit 0
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-bind-warning.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

pass=0
fail=0
check() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $desc"; pass=$((pass+1))
  else
    echo "  FAIL: $desc"; echo "    expected: |$expected|"; echo "    actual:   |$actual|"
    fail=$((fail+1))
  fi
}

mkdir -p "$TMP/srv" "$TMP/keys" "$TMP/cc"

HAVE_TLS=0
if command -v openssl >/dev/null 2>&1 \
   && openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
        -keyout "$TMP/key.pem" -out "$TMP/cert.pem" -subj "/CN=localhost" \
        >/dev/null 2>&1; then
  HAVE_TLS=1
fi

HAVE_KEYS=0
DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/throwaway.db" \
  "SELECT dolt_creds_new();" >/dev/null 2>&1
if cp "$TMP"/cc/*.jwk "$TMP/keys/" 2>/dev/null; then
  HAVE_KEYS=1
fi

# The warnings are emitted before bind(), so an address the host has no
# loopback alias for (127.1.2.3 off Linux) still exercises the decision.
warnings_for() {
  local err="$TMP/err.txt" out="$TMP/out.txt" pid i
  : >"$err"; : >"$out"
  "$REMOTESRV" -p 0 "$@" "$TMP/srv" >"$out" 2>"$err" &
  pid=$!
  for i in $(seq 1 40); do
    kill -0 "$pid" 2>/dev/null || break
    grep -q 'serving' "$out" 2>/dev/null && break
    sleep 0.1
  done
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  cat "$err"
}

tls_count()  { printf '%s' "$1" | grep -c 'without TLS' || true; }
auth_count() { printf '%s' "$1" | grep -c 'without authentication' || true; }

echo "=== 1. Loopback binds warn about nothing ==="
w=$(warnings_for)
check "default bind: no TLS warning"  "0" "$(tls_count "$w")"
check "default bind: no auth warning" "0" "$(auth_count "$w")"

w=$(warnings_for --bind 127.0.0.1)
check "127.0.0.1: no TLS warning"  "0" "$(tls_count "$w")"
check "127.0.0.1: no auth warning" "0" "$(auth_count "$w")"

echo "=== 2. All of 127/8 is loopback, not just 127.0.0.1 ==="
w=$(warnings_for --bind 127.1.2.3)
check "127.1.2.3: no TLS warning"  "0" "$(tls_count "$w")"
check "127.1.2.3: no auth warning" "0" "$(auth_count "$w")"

echo "=== 3. Public bind with neither TLS nor auth warns about both ==="
w=$(warnings_for --bind 0.0.0.0)
check "0.0.0.0 bare: TLS warning"  "1" "$(tls_count "$w")"
check "0.0.0.0 bare: auth warning" "1" "$(auth_count "$w")"

echo "=== 3b. --auth-keys without --audience refuses to start ==="
if [ "$HAVE_KEYS" = 1 ]; then
  : >"$TMP/err.txt"; : >"$TMP/out.txt"
  "$REMOTESRV" -p 0 --bind 0.0.0.0 --auth-keys "$TMP/keys" "$TMP/srv" \
    >"$TMP/out.txt" 2>"$TMP/err.txt" || true
  if grep -q 'audience is required' "$TMP/err.txt"; then
    check "auth-keys without audience errors" "1" "1"
  else
    check "auth-keys without audience errors" "1" "0"
    echo "    stderr: $(tr '\n' ' ' < "$TMP/err.txt")"
  fi
else
  echo "  SKIP: could not generate a credential"
fi

echo "=== 4. Configured auth is not reported as unauthenticated ==="
if [ "$HAVE_KEYS" = 1 ]; then
  w=$(warnings_for --bind 0.0.0.0 --auth-keys "$TMP/keys" --audience 0.0.0.0)
  check "0.0.0.0 with --auth-keys: still warns about TLS" "1" "$(tls_count "$w")"
  check "0.0.0.0 with --auth-keys: no auth warning"       "0" "$(auth_count "$w")"
else
  echo "  SKIP: could not generate a credential"
fi

echo "=== 5. TLS does not silence the authentication warning ==="
if [ "$HAVE_TLS" = 1 ]; then
  w=$(warnings_for --bind 0.0.0.0 --cert "$TMP/cert.pem" --key "$TMP/key.pem")
  check "0.0.0.0 with TLS only: no TLS warning" "0" "$(tls_count "$w")"
  check "0.0.0.0 with TLS only: auth warning"   "1" "$(auth_count "$w")"
else
  echo "  SKIP: openssl unavailable, cannot build a certificate"
fi

echo "=== 6. Fully configured public bind is quiet ==="
if [ "$HAVE_TLS" = 1 ] && [ "$HAVE_KEYS" = 1 ]; then
  w=$(warnings_for --bind 0.0.0.0 --cert "$TMP/cert.pem" --key "$TMP/key.pem" \
                   --auth-keys "$TMP/keys" --audience 0.0.0.0)
  check "0.0.0.0 with TLS and auth: no TLS warning"  "0" "$(tls_count "$w")"
  check "0.0.0.0 with TLS and auth: no auth warning" "0" "$(auth_count "$w")"
else
  echo "  SKIP: needs both a certificate and a credential"
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
