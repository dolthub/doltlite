/* BLOBKEY sort key encoding: type-prefixed (NULL/NUM/TEXT/BLOB) for memcmp ordering. */
#ifndef SQLITE_SORTKEY_H
#define SQLITE_SORTKEY_H

#include "sqliteInt.h"

#define SORTKEY_NULL    0x05
#define SORTKEY_NUM     0x15  
#define SORTKEY_TEXT    0x35
#define SORTKEY_BLOB    0x45

int sortKeyFromRecord(const u8 *pRec, int nRec, u8 **ppOut, int *pnOut);

/*
** Encode the first nKeyField columns of pRec as a sort key. This is used
** for the prolly tree key of WITHOUT ROWID table btrees, where the prolly
** key needs to identify the row by its primary key columns only (so that
** a row UPDATE that changes a non-PK column produces the same key on
** both sides of a diff and can be classified as MODIFY rather than
** DELETE+ADD). The remaining columns are stored in the prolly value side
** by the caller. nKeyField=0 encodes the whole record (same as
** sortKeyFromRecord).
*/
int sortKeyFromRecordPrefix(const u8 *pRec, int nRec, int nKeyField,
                            u8 **ppOut, int *pnOut);

/*
** Collation-aware variant. When pKeyInfo is non-NULL, each encoded TEXT
** column is transformed according to its declared collation before
** being written into the sort key:
**
**   BINARY  — no transform (default)
**   NOCASE  — ASCII A-Z folded to a-z so 'Alice' and 'alice' produce
**             the same sort key
**   RTRIM   — trailing 0x20 bytes stripped so 'abc' and 'abc ' produce
**             the same sort key
**
** Any other collation (user-defined, etc.) falls back to BINARY because
** the prolly tree can't pair-wise invoke xCmp at key-encode time. Pass
** pKeyInfo=NULL to get the legacy byte-level encoding.
*/
int sortKeyFromRecordPrefixColl(const u8 *pRec, int nRec, int nKeyField,
                                 const KeyInfo *pKeyInfo,
                                 u8 **ppOut, int *pnOut);

int sortKeySize(const u8 *pRec, int nRec);

int recordFromSortKey(const u8 *pSortKey, int nSortKey, u8 **ppOut, int *pnOut);
int recordFromSortKeyBuffer(
  const u8 *pSortKey, int nSortKey,
  u8 **ppBuf, int *pnAlloc, int *pnOut
);

static inline int compareSortKeys(
  const u8 *pKey1, int nKey1,
  const u8 *pKey2, int nKey2
){
  int n = nKey1 < nKey2 ? nKey1 : nKey2;
  int c = memcmp(pKey1, pKey2, n);
  if( c!=0 ) return c;
  if( nKey1 < nKey2 ) return -1;
  if( nKey1 > nKey2 ) return 1;
  return 0;
}

#endif 
