
#ifndef DOLTLITE_RECORD_H
#define DOLTLITE_RECORD_H

#include "record_codec.h"

i64 doltliteSyntheticRowidFromRecord(const u8 *pRec, int nRec,
    const KeyInfo *pKeyInfo);
i64 doltliteSyntheticRowidFromSortKey(const u8 *pSortKey, int nSortKey,
    const KeyInfo *pKeyInfo);

#endif
