# BLAKE3 (vendored)

`prollyHashCompute` uses BLAKE3 to derive 20-byte content addresses for
prolly chunks. The portable C reference implementation lives in this
directory.

## Provenance

| | |
|---|---|
| Upstream | https://github.com/BLAKE3-team/BLAKE3 |
| Version  | 1.8.5 |
| Files vendored | `blake3.h`, `blake3.c`, `blake3_portable.c` (verbatim from upstream `c/`) |
| Files modified | `blake3_impl.h` — declarations for the SSE/AVX/NEON paths stripped, since DoltLite ships the portable implementation only |
| Files DoltLite-original | `blake3_dispatch_portable.c` — replaces upstream `blake3_dispatch.c`. Removes runtime CPU feature detection; always calls the portable functions. ~30 lines. |
| License | Apache 2.0 with LLVM exception, OR CC0 1.0 (dual-licensed by upstream). DoltLite redistributes under Apache 2.0 (project-wide). See `LICENSE`. |

## Why portable, not SIMD

DoltLite ships to platforms where the BLAKE3 SIMD paths don't apply:

- **WebAssembly**: no SSE/AVX/NEON intrinsics
- **iOS / Android**: ARM64-NEON works there but the same source needs to compile on x86_64 too, and we want one source set
- **Cross-platform consistency**: portable BLAKE3 produces identical hashes on every platform, by definition

The portable path measures ~2.6× faster than the SHA-512 it replaced. SIMD would be another ~2× on top — that's a separate follow-up gated behind a compile-time flag.

## Updating

To pull in a newer BLAKE3 release:

1. Replace `blake3.h`, `blake3.c`, `blake3_portable.c` with their current upstream versions verbatim.
2. Re-apply the `blake3_impl.h` modification: drop the SIMD-only declarations (everything under `#if defined(IS_X86)` and `#if BLAKE3_USE_NEON == 1`).
3. Set `MAX_SIMD_DEGREE` to 1 unconditionally.
4. Verify `blake3_dispatch_portable.c` still satisfies the function signatures declared in `blake3_impl.h`. If upstream adds a new dispatched function, mirror it in the shim.
5. Run `bash test/blake3_kat_test.sh` to confirm hash output is unchanged.
6. Update the version table above.

If upstream ever changes the BLAKE3 algorithm itself (extremely unlikely; it would be BLAKE4), it'd require a `CHUNK_STORE_VERSION` bump for format compatibility.

## Test vectors

`test/blake3_kat_test.sh` runs a small KAT (Known Answer Test) suite against this vendored copy. Reference values were generated against the upstream-supplied libblake3 binary and cross-checked against the canonical empty-input vector published in the BLAKE3 spec.
