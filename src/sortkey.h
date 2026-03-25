/*
** Sort key encoding for BLOBKEY (index) entries.
**
** Converts SQLite serialized record format into a byte string where
** memcmp produces the correct SQLite type-affinity sort order:
**   NULL < INTEGER < FLOAT < TEXT < BLOB
**
** This eliminates the need for field-by-field record parsing during
** comparison, replacing it with a single memcmp call.
**
** Sort key format per field:
**   NULL:    0x05
**   Numeric: 0x15 + 8-byte IEEE 754 double with sign normalization
**            (integers are converted to double for uniform encoding)
**   Text:    0x35 + raw bytes + 0x00 0x00 terminator
**   Blob:    0x45 + raw bytes + 0x00 0x00 terminator
**
** Tag values are spaced to allow future additions.
** Text and blob use double-NUL terminator with 0x00→0x00 0x01 escaping
** so embedded NULs sort correctly.
*/
#ifndef SQLITE_SORTKEY_H
#define SQLITE_SORTKEY_H

#include "sqliteInt.h"

/* Type tags — spaced so memcmp gives correct affinity ordering */
#define SORTKEY_NULL    0x05
#define SORTKEY_NUM     0x15  /* All numbers (int + float) encoded as double */
#define SORTKEY_TEXT    0x35
#define SORTKEY_BLOB    0x45

/*
** Compute the sort key for a SQLite serialized record.
**
** pRec/nRec:  Input SQLite record (varint header + field data)
** ppOut:      Output sort key (caller must sqlite3_free)
** pnOut:      Output sort key size
**
** Returns SQLITE_OK on success, SQLITE_NOMEM on allocation failure,
** SQLITE_CORRUPT if the record is malformed.
*/
int sortKeyFromRecord(const u8 *pRec, int nRec, u8 **ppOut, int *pnOut);

/*
** Compute the sort key size without allocating.
** Returns the number of bytes needed, or -1 on malformed input.
*/
int sortKeySize(const u8 *pRec, int nRec);

/*
** Decode a sort key back into a SQLite serialized record.
** The encoding is fully lossless — the reconstructed record is
** byte-for-byte equivalent to a canonical SQLite record encoding.
**
** pSortKey/nSortKey: Input sort key
** ppOut:             Output SQLite record (caller must sqlite3_free)
** pnOut:             Output record size
**
** Returns SQLITE_OK on success, SQLITE_NOMEM on allocation failure,
** SQLITE_CORRUPT if the sort key is malformed.
*/
int recordFromSortKey(const u8 *pSortKey, int nSortKey, u8 **ppOut, int *pnOut);

/*
** Compare two sort keys. This is just memcmp with length handling,
** provided as a convenience for readability.
*/
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

#endif /* SQLITE_SORTKEY_H */
