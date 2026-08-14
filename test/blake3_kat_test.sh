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


echo "=== BLAKE3 KAT (vendored) ==="
HERE=$(cd "$(dirname "$0")/.." && pwd)
BIN="${DOLTLITE_BLAKE3_KAT_BIN:-}"
if [ -z "$BIN" ]; then
  BIN="$TMP/blake3_kat"
  blake_objects=(blake3.o blake3_portable.o blake3_dispatch.o)
  for object in blake3_sse2.o blake3_sse41.o blake3_avx2.o blake3_avx512.o blake3_neon.o; do
    if [ -f "$object" ]; then
      blake_objects+=("$object")
    fi
  done
  "${CC:-cc}" ${CFLAGS:-"-O2"} "$HERE/test/blake3_kat.c" \
     prolly_hash.o "${blake_objects[@]}" \
     -I "$HERE/src" -I "$HERE/ext/blake3" -DDOLTLITE_PROLLY=1 \
     ${LDFLAGS:-} -lm \
     -o "$BIN" 2>"$TMP/build.err" || {
      echo "  build failed:"
      cat "$TMP/build.err"
      exit 1
    }
fi
if [ ! -x "$BIN" ]; then
  echo "BLAKE3 KAT binary not found at $BIN"
  exit 1
fi

check "empty (32-byte BLAKE3)" \
  "$($BIN empty-blake3-32)" \
  "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"

check "prollyHashCompute(empty)" \
  "$($BIN prolly-empty)" \
  "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9"

check "prollyHashCompute(\"abc\")" \
  "$($BIN prolly-abc)" \
  "6437b3ac38465133ffb63b75273a8db548c55846"

check "prollyHashCompute(0..255 x 4, 1024 B = 1 chunk)" \
  "$($BIN prolly-1024)" \
  "882179b8dbccd285cda241d968cfcccb3156c5ed"

check "prollyHashCompute(0..255 x 16, 4096 B = 4 chunks)" \
  "$($BIN prolly-4096)" \
  "0b3dda6fbfe01c93d79388632f66c5c1fa781382"

check "prollyHashCompute(0..255 x 64, 16384 B = 16 chunks)" \
  "$($BIN prolly-16384)" \
  "d49d367e4b0011a34510a28a1eb0caeb3e51e77f"

# Chunk counts that do not fill a SIMD batch on their own. These are the
# ordinary prolly node sizes, and they reach hash_many() padded out to a lane
# count with a dummy chunk; the pinned values are the unpadded ones.
check "prollyHashCompute(2048 B = 2 chunks)" \
  "$($BIN prolly-size 2048)" \
  "1bdccfde0210a8ca178be19c6777cdb4b9a8fd24"

check "prollyHashCompute(3712 B = 3 chunks + partial)" \
  "$($BIN prolly-size 3712)" \
  "1a9a7255bb8549b589da5fadd01078a420836777"

check "prollyHashCompute(5120 B = 5 chunks)" \
  "$($BIN prolly-size 5120)" \
  "755ae1e177565c02059c4eb273c1e1765f4aebfd"

echo
if [ "$fail" -eq 0 ]; then
  echo "=== Results: $pass passed, 0 failed ==="
  exit 0
else
  echo "=== Results: $pass passed, $fail failed ==="
  exit 1
fi
