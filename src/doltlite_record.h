
#ifndef DOLTLITE_RECORD_H
#define DOLTLITE_RECORD_H

#include "record_codec.h"

int doltliteSyntheticRowidFromRecord(const u8 *pRec, int nRec,
    const KeyInfo *pKeyInfo, i64 *pRowid);
int doltliteSyntheticRowidFromSortKey(const u8 *pSortKey, int nSortKey,
    const KeyInfo *pKeyInfo, i64 *pRowid);

#endif
