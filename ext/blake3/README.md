# BLAKE3 (vendored)

`prollyHashCompute` uses BLAKE3 to derive 20-byte content addresses for
prolly chunks. The vendored sources live in this directory.

## Provenance

| | |
|---|---|
| Upstream | https://github.com/BLAKE3-team/BLAKE3 |
| Version  | 1.8.5 |
| Files vendored | `blake3.h`, `blake3.c`, `blake3_portable.c`, `blake3_impl.h`, `blake3_dispatch.c`, `blake3_sse2.c`, `blake3_sse41.c`, `blake3_avx2.c`, `blake3_avx512.c`, `blake3_neon.c` (verbatim from upstream `c/`, with an SPDX header prepended) |
| Files modified | `blake3.c` and `blake3.h` have the BLAKE3_USE_TBB threading code stripped — DoltLite doesn't link TBB |
| License | Apache 2.0 with LLVM exception, OR CC0 1.0 (dual-licensed by upstream). DoltLite redistributes under Apache 2.0 (project-wide). See `LICENSE`. |

## SIMD paths

`blake3_dispatch.c` selects the fastest available implementation at
runtime via CPUID on x86 and AArch64 detection:

| Target | Backends compiled in | Picked at runtime |
|---|---|---|
| x86_64    | SSE2, SSE4.1, AVX2, AVX-512 | best one supported by the CPU |
| aarch64   | NEON                        | NEON (always — it's part of the ARMv8 baseline) |
| wasm32, riscv, … | (none)               | portable |

`main.mk` decides which `.c` files to compile by inspecting
`$(B.cc) -dumpmachine`, so cross-compilation to wasm via emcc
correctly produces a portable-only binary even on an x86_64 host.

Per-file `-msse2 / -msse4.1 / -mavx2 / -mavx512f -mavx512vl` flags
are scoped to their respective `.c` files; the rest of the tree is
compiled at the build's baseline ISA. Runtime dispatch never calls
into a backend the CPU doesn't support, so this is safe.

NEON measures ~2.2× faster than portable on Apple M-series silicon
(~770 MB/s → ~1700 MB/s on a 16 KB buffer). On x86 hardware AVX-512
is the typical winner; SSE4.1 is the conservative fallback.

## Updating

To pull in a newer BLAKE3 release:

1. Replace `blake3.h`, `blake3.c`, `blake3_portable.c`,
   `blake3_impl.h`, `blake3_dispatch.c`, and the five SIMD `.c` files
   with their current upstream versions verbatim. Re-prepend the
   SPDX header used here.
2. Re-strip BLAKE3_USE_TBB references from `blake3.c` and `blake3.h`
   if they still appear and you don't intend to link TBB.
3. If upstream adds a new SIMD path or renames a function, update
   `main.mk`'s arch-detection block.
4. Run `bash test/blake3_kat_test.sh` to confirm hash output is
   unchanged, then run a microbench on x86 and aarch64 hosts.
5. Update the version table above.

If upstream ever changes the BLAKE3 algorithm itself (extremely
unlikely; it would be BLAKE4), it'd require a `CHUNK_STORE_VERSION`
bump for format compatibility.

## Test vectors

`test/blake3_kat_test.sh` runs a small KAT (Known Answer Test) suite
against this vendored copy. Reference values were generated against
the upstream-supplied libblake3 binary and cross-checked against the
canonical empty-input vector published in the BLAKE3 spec. The test
links whatever SIMD object set the build produced for the host, so
on aarch64 it covers the NEON path and on x86 it covers the highest
ISA the build machine supports.
