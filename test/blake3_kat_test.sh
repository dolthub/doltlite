#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT

if [ ! -x "$DOLTLITE" ]; then
  echo "doltlite binary not found at $DOLTLITE"
  exit 1
fi

pass=0
fail=0
check() {
  local name="$1"
  local got="$2"
  local want="$3"
  if [ "$got" = "$want" ]; then
    pass=$((pass+1))
    echo "  PASS  $name"
  else
    fail=$((fail+1))
    echo "  FAIL  $name"
    echo "        want: $want"
    echo "        got:  $got"
  fi
}

hash_via_doltlite() {
  local input_hex="$1"
  rm -f "$TMP/khash.db"
  local out
  out=$(printf "SELECT lower(hex(unhex(dolt_hashof_bytes(unhex('%s')))));\n" "$input_hex" \
        | "$DOLTLITE" "$TMP/khash.db" 2>/dev/null)
  echo "$out"
}


cat > "$TMP/blake3_kat.c" <<'EOF'
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "blake3.h"

typedef struct ProllyHash { uint8_t data[20]; } ProllyHash;
extern void prollyHashCompute(const void *pData, int nData, ProllyHash *pOut);

static void print_hex(const uint8_t *p, int n) {
  for (int i = 0; i < n; i++) printf("%02x", p[i]);
}

int main(int argc, char **argv) {
  if (argc < 2) { fprintf(stderr, "usage: %s <test>\n", argv[0]); return 2; }
  const char *test = argv[1];

  if (!strcmp(test, "empty-blake3-32")) {
    /* full 32-byte BLAKE3 digest of the empty input */
    blake3_hasher h; uint8_t out[32];
    blake3_hasher_init(&h);
    blake3_hasher_update(&h, "", 0);
    blake3_hasher_finalize(&h, out, 32);
    print_hex(out, 32); printf("\n"); return 0;
  }

  if (!strcmp(test, "prolly-empty")) {
    ProllyHash h; prollyHashCompute("", 0, &h);
    print_hex(h.data, 20); printf("\n"); return 0;
  }
  if (!strcmp(test, "prolly-abc")) {
    ProllyHash h; prollyHashCompute("abc", 3, &h);
    print_hex(h.data, 20); printf("\n"); return 0;
  }
  if (!strcmp(test, "prolly-1024")) {
    uint8_t buf[1024];
    for (int i = 0; i < 1024; i++) buf[i] = (uint8_t)(i % 256);
    ProllyHash h; prollyHashCompute(buf, 1024, &h);
    print_hex(h.data, 20); printf("\n"); return 0;
  }
  if (!strcmp(test, "prolly-4096")) {
    uint8_t *buf = malloc(4096);
    for (int i = 0; i < 4096; i++) buf[i] = (uint8_t)(i % 256);
    ProllyHash h; prollyHashCompute(buf, 4096, &h);
    free(buf);
    print_hex(h.data, 20); printf("\n"); return 0;
  }
  if (!strcmp(test, "prolly-16384")) {
    uint8_t *buf = malloc(16384);
    for (int i = 0; i < 16384; i++) buf[i] = (uint8_t)(i % 256);
    ProllyHash h; prollyHashCompute(buf, 16384, &h);
    free(buf);
    print_hex(h.data, 20); printf("\n"); return 0;
  }
  fprintf(stderr, "unknown test: %s\n", test);
  return 2;
}
EOF

echo "=== BLAKE3 KAT (vendored) ==="
HERE=$(cd "$(dirname "$0")/.." && pwd)
cc -O2 "$TMP/blake3_kat.c" \
   prolly_hash.o blake3*.o \
   -I "$HERE/src" -I "$HERE/ext/blake3" -DDOLTLITE_PROLLY=1 \
   -lm \
   -o "$TMP/blake3_kat" 2>"$TMP/build.err" || {
    echo "  build failed:"
    cat "$TMP/build.err"
    exit 1
  }

check "empty (32-byte BLAKE3)" \
  "$($TMP/blake3_kat empty-blake3-32)" \
  "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"

check "prollyHashCompute(empty)" \
  "$($TMP/blake3_kat prolly-empty)" \
  "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9"

check "prollyHashCompute(\"abc\")" \
  "$($TMP/blake3_kat prolly-abc)" \
  "6437b3ac38465133ffb63b75273a8db548c55846"

check "prollyHashCompute(0..255 x 4, 1024 B = 1 chunk)" \
  "$($TMP/blake3_kat prolly-1024)" \
  "882179b8dbccd285cda241d968cfcccb3156c5ed"

check "prollyHashCompute(0..255 x 16, 4096 B = 4 chunks)" \
  "$($TMP/blake3_kat prolly-4096)" \
  "0b3dda6fbfe01c93d79388632f66c5c1fa781382"

check "prollyHashCompute(0..255 x 64, 16384 B = 16 chunks)" \
  "$($TMP/blake3_kat prolly-16384)" \
  "d49d367e4b0011a34510a28a1eb0caeb3e51e77f"

echo
if [ "$fail" -eq 0 ]; then
  echo "=== Results: $pass passed, 0 failed ==="
  exit 0
else
  echo "=== Results: $pass passed, $fail failed ==="
  exit 1
fi
