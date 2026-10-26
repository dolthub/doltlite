#!/usr/bin/env bash
set -euo pipefail
build_dir="${1:-build}"
root="$(cd "$(dirname "$0")/.." && pwd)"
source "$root/test/lib/openssl_test_cert.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
openssl_test_cert "$tmp/key.pem" "$tmp/cert.pem" /CN=localhost \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' >/dev/null 2>&1
export DOLTLITE_CA_FILE="$tmp/cert.pem"
export DOLTLITE_CREDS_DIR="$tmp/creds"
unset DOLTLITE_CREDS_KID
"$build_dir/doltlite" :memory: 'SELECT dolt_creds_new();' >/dev/null
"$build_dir/http_transfer_test" "$tmp/cert.pem" "$tmp/key.pem"
echo __SUITE_COMPLETE__
