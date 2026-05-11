
#ifndef SQLITE_PROLLY_XXHASH_H
#define SQLITE_PROLLY_XXHASH_H

#include "sqliteInt.h"

u32 prollyXXH32(const u8 *p, int n, u32 seed);

#endif
