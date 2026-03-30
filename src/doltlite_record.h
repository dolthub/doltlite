/*
** Shared record parsing utilities for doltlite feature files.
** SQLite record format: varint(header_size) + serial_types + field_data.
*/
#ifndef DOLTLITE_RECORD_H
#define DOLTLITE_RECORD_H

#include "sqliteInt.h"

/*
** Decode a SQLite record-format blob into a pipe-separated text string.
** Caller must sqlite3_free() the result.
** Returns NULL on error or empty input.
**
** Example: a record with (integer 3, text 'hello') -> "3|hello"
*/
char *doltliteDecodeRecord(const u8 *pData, int nData);

/*
** Set a sqlite3_context result to the decoded text of a record blob,
** or NULL if the blob is empty/NULL.
*/
void doltliteResultRecord(sqlite3_context *ctx, const u8 *pData, int nData);

/*
** Shared column-info structure used by diff_table, history, and at modules.
*/
typedef struct DoltliteColInfo DoltliteColInfo;
struct DoltliteColInfo {
  char **azName;    /* Column names (owned) */
  int nCol;         /* Number of columns (ALL columns including PK) */
  int iPkCol;       /* Index of the INTEGER PRIMARY KEY column, or -1 */
};

/*
** Populate ci with column names from the given table via PRAGMA table_info.
** Caller must call doltliteFreeColInfo() when done.
*/
int doltliteGetColumnNames(sqlite3 *db, const char *zTable, DoltliteColInfo *ci);

/*
** Free the column name array inside ci.
*/
void doltliteFreeColInfo(DoltliteColInfo *ci);

/*
** Read a SQLite varint from pBuf. Returns bytes consumed (1-9).
** Sets *pVal to the decoded value.
*/
static inline int dlReadVarint(const u8 *p, const u8 *pEnd, u64 *pVal){
  u64 v;
  int i;
  if( p >= pEnd ){ *pVal = 0; return 0; }
  v = p[0];
  if( !(v & 0x80) ){ *pVal = v; return 1; }
  v &= 0x7f;
  for(i = 1; i < 9 && p+i < pEnd; i++){
    v = (v << 7) | (p[i] & 0x7f);
    if( !(p[i] & 0x80) ){ *pVal = v; return i + 1; }
  }
  *pVal = v;
  return i;
}

/*
** Return the payload byte length for a given SQLite serial type.
** Types: 0=NULL(0), 1-6=int(1,2,3,4,6,8), 7=float(8),
** 8=false(0), 9=true(0), N>=12=blob((N-12)/2), N>=13=text((N-13)/2)
*/
static inline int dlSerialTypeLen(u64 st){
  static const u8 aLen[] = {0, 1, 2, 3, 4, 6, 8};
  if( st <= 6 ) return aLen[st];
  if( st == 7 ) return 8;
  if( st >= 12 ) return (int)(st - 12) / 2;
  return 0;
}

#endif /* DOLTLITE_RECORD_H */
