#!/usr/bin/env bash
set -euo pipefail

platform="${1:?platform required}"
source ../.github/scripts/parallel-compile.sh
case "$platform" in
  macos) ci_compile_init "${DOLTLITE_PROBE_JOBS:-$(sysctl -n hw.logicalcpu)}" ;;
  ubuntu) ci_compile_init "${DOLTLITE_PROBE_JOBS:-2}" ;;
  *) exit 1 ;;
esac
mkdir -p ci-probe-objects
creds_objects=()
for source in ../test/sqlite3_alloc_stub.c ../src/doltlite_creds.c \
              ../ext/ed25519/{fe,ge,sc,sha512,keypair,sign,verify,add_scalar}.c; do
  object="ci-probe-objects/creds-${source##*/}.o"
  ci_compile cc $CFLAGS -Wall -I../src -I../ext/ed25519 -c "$source" -o "$object"
  creds_objects+=("$object")
done
for name in doltlite_creds_kat creds_verify_kat; do
  ci_compile cc $CFLAGS -Wall -I../src -I../ext/ed25519 \
    -c "../test/$name.c" -o "ci-probe-objects/$name.o"
done
tls_objects=()
for source in ../test/doltlite_tls_test_main.c ../test/sqlite3_alloc_stub.c \
              ../src/doltlite_tls.c ../ext/mbedtls/library/*.c; do
  object="ci-probe-objects/tls-${source##*/}.o"
  ci_compile cc $CFLAGS -I../src -I../ext/mbedtls/include -c "$source" -o "$object"
  tls_objects+=("$object")
done
ci_compile cc $CFLAGS -I../src -I../ext/blake3 -DDOLTLITE_PROLLY=1 \
  -c ../test/blake3_kat.c -o ci-probe-objects/blake3_kat.o
ci_compile cc $CFLAGS -Wno-comment -I. \
  -c ../test/amalgamation_http_probe.c -o ci-probe-objects/amalgamation_http_probe.o
probe_libs=(-lz -lpthread -lm)
probe_amalgamation=ci-amalgamation.o
if [ "$platform" = ubuntu ]; then
  probe_libs+=(-ldl)
else
  probe_amalgamation=ci-probe-objects/amalgamation.o
  ci_compile cc $CFLAGS -Wno-comment -I. -c sqlite3.c -o "$probe_amalgamation"
fi
ci_compile_wait
ci_compile cc $CFLAGS -Wall ci-probe-objects/doltlite_creds_kat.o "${creds_objects[@]}" -o creds_kat
ci_compile cc $CFLAGS -Wall ci-probe-objects/creds_verify_kat.o "${creds_objects[@]}" -o creds_verify_kat
ci_compile cc $CFLAGS "${tls_objects[@]}" -o tls_test
blake_objects=(blake3.o blake3_portable.o blake3_dispatch.o)
for object in blake3_sse2.o blake3_sse41.o blake3_avx2.o blake3_avx512.o blake3_neon.o; do
  [ ! -f "$object" ] || blake_objects+=("$object")
done
ci_compile cc $CFLAGS ci-probe-objects/blake3_kat.o prolly_hash.o "${blake_objects[@]}" -lm -o blake3_kat
ci_compile cc $CFLAGS -Wno-comment ci-probe-objects/amalgamation_http_probe.o "$probe_amalgamation" \
  "${probe_libs[@]}" -o amalgamation_http_probe
ci_compile_wait
rm -rf ci-probe-objects
rm -f ci-amalgamation.o
