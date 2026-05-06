
#ifndef SQLITE_PROLLY_XXHASH_H
#define SQLITE_PROLLY_XXHASH_H

#include "sqliteInt.h"

/* XXH32: 32-bit non-cryptographic hash by Yann Collet.
**
** Used by the prolly chunker to derive a uniform u32 from a key, which
** the Weibull-distribution chunk-boundary check (prollyWeibullCheck)
** treats as a uniform random number in [0, 2^32).
**
** Salting is by `seed`. The chunker passes the tree level as seed so
** boundaries at different levels don't align on the same keys.
**
** Adapted from upstream xxHash (Yann Collet, BSD-2-Clause). */
u32 prollyXXH32(const u8 *p, int n, u32 seed);

#endif
