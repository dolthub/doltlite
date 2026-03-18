/*
** SQLite record format decoder for Doltlite.
**
** Decodes binary SQLite record blobs into human-readable text.
** Used by dolt_diff, dolt_diff_<table>, dolt_history_<table>, dolt_at.
*/
#ifndef DOLTLITE_RECORD_H
#define DOLTLITE_RECORD_H

#include "sqliteInt.h"

/*
** Decode a SQLite record-format blob into a pipe-separated text string.
** Caller must sqlite3_free() the result.
** Returns NULL on error or empty input.
**
** Example: a record with (integer 3, text 'hello') → "3|hello"
*/
char *doltliteDecodeRecord(const u8 *pData, int nData);

/*
** Set a sqlite3_context result to the decoded text of a record blob,
** or NULL if the blob is empty/NULL.
*/
void doltliteResultRecord(sqlite3_context *ctx, const u8 *pData, int nData);

#endif /* DOLTLITE_RECORD_H */
