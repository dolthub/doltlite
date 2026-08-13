# BLAKE3 (vendored)

`prollyHashCompute` uses BLAKE3 to derive 20-byte content addresses for
prolly chunks. The vendored sources live in this directory.

## Provenance

| | |
|---|---|
| Upstream | https://github.com/BLAKE3-team/BLAKE3 |
| Version  | 1.8.5 |
| Files vendored | `blake3.h`, `blake3.c`, `blake3_portable.c`, `blake3_impl.h`, `blake3_dispatch.c`, `blake3_sse2.c`, `blake3_sse41.c`, `blake3_avx2.c`, `blake3_avx512.c`, `blake3_neon.c` (verbatim from upstream `c/`, with an SPDX header prepended) |
| Files modified | `blake3.c` and `blake3.h` have the BLAKE3_USE_TBB threading code stripped — DoltLite doesn't link TBB, plus the two local additions under *Small-buffer throughput* below |
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

## Small-buffer throughput (local additions)

Upstream's SIMD reaches those numbers only on buffers large enough to
fill a lane group. A prolly node averages ~3.7 KB — three 1 KiB BLAKE3
chunks plus a partial one — and upstream hashes it at portable speed:

- `blake3_hasher_update()` consumes input as a stream, so it splits a
  buffer into power-of-two aligned subtrees. 3.7 KB becomes a 2-chunk
  subtree, then a 1-chunk subtree, then a partial chunk.
- Every `blake3_hash_many_*()` backend vectorises whole groups of its
  lane count and hashes the remainder one chunk at a time, so a batch
  of 2 or 3 never reaches the vector path at all.

Two additions fix that, and neither changes any hash value:

1. `blake3_hash_oneshot()` in `blake3.c` (declared in `blake3.h`) hands
   a whole buffer to the wide compressor in one call, so every full
   chunk lands in a single `hash_many()` batch. `prollyHashCompute()`
   calls it.
2. `compress_chunks_parallel()` rounds that batch up to the next lane
   count with a dummy chunk whose chaining value is discarded — at
   worst the cost of the serial remainder it replaces.

Measured on Apple M1 Pro (NEON, 4 lanes): 3712 B 781 → 1165 MB/s,
3072 B 785 → 1300 MB/s, 2048 B 795 → 900 MB/s; 4096 B and above are
unchanged because they already filled a lane group. x86 sees the same
effect at SSE4.1's 4 lanes, and additionally pulls 5–7 chunk buffers
onto AVX2's 8.

Both are verified output-identical to the pristine vendored code for
every input length from 0 to 40000 bytes.

## Updating

To pull in a newer BLAKE3 release:

1. Replace `blake3.h`, `blake3.c`, `blake3_portable.c`,
   `blake3_impl.h`, `blake3_dispatch.c`, and the five SIMD `.c` files
   with their current upstream versions verbatim. Re-prepend the
   SPDX header used here.
2. Re-strip BLAKE3_USE_TBB references from `blake3.c` and `blake3.h`
   if they still appear and you don't intend to link TBB, and re-apply
   the two *Small-buffer throughput* additions (they are marked
   `DOLTLITE:` in the source).
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
