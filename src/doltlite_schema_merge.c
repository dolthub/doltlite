/*
** Schema merge helpers. Extracted from doltlite.c.
**
** Functions that handle data migration after a schema merge:
**   extractColNameFromDef()  - parse column name from ADD COLUMN def
**   doltliteBindField()        - bind a field from a SQLite record blob
**   migrateDiffCb()          - diff callback for schema data migration
**   migrateSchemaRowData()   - top-level migration driver
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "prolly_diff.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"

#include <string.h>
#include <ctype.h>

static int dlIdentTokenLen(const char *z, int n){
  int i = 0;
  if( !z || n<=0 ) return 0;
  if( z[0]=='"' || z[0]=='`' ){
    char q = z[0];
    for(i=1; i<n; i++){
      if( z[i]==q ){
        if( i+1<n && z[i+1]==q ){
          i++;
          continue;
        }
        return i+1;
      }
    }
    return n;
  }
  if( z[0]=='[' ){
    for(i=1; i<n; i++){
      if( z[i]==']' ){
        if( i+1<n && z[i+1]==']' ){
          i++;
          continue;
        }
        return i+1;
      }
    }
    return n;
  }
  while( i<n && !isspace((unsigned char)z[i]) && z[i]!='(' && z[i]!=',' ){
    i++;
  }
  return i;
}

static char *dlExtractIdentLower(const char *z, int n){
  char *zName;
  int i;
  if( !z || n<=0 ) return 0;
  zName = sqlite3_malloc(n + 1);
  if( !zName ) return 0;
  memcpy(zName, z, n);
  zName[n] = 0;
  sqlite3Dequote(zName);
  for(i=0; zName[i]; i++){
    zName[i] = (char)tolower((unsigned char)zName[i]);
  }
  return zName;
}

/*
** Extract the column name from an ADD COLUMN definition string like "y INTEGER".
** Returns a malloc'd, lowercased copy of the name. Caller must sqlite3_free.
*/
char *extractColNameFromDef(const char *zDef){
  const char *s = zDef;
  int len;

  /* Skip leading whitespace */
  while( *s && isspace((unsigned char)*s) ) s++;
  if( !*s ) return 0;

  len = dlIdentTokenLen(s, (int)strlen(s));
  return dlExtractIdentLower(s, len);
}

/*
** Bind a field from a SQLite record-format blob to a prepared statement
** parameter. The field is identified by its serial type and offset within
** the record data.
*/
/* bindRecordField removed — use doltliteBindField() from doltlite_record.c */

/*
** Diff callback for schema data migration. Called for each row that
** changed between ancestor and theirs (ADD or MODIFY). Extracts the
** added column values from theirs' record and UPDATEs the merged table.
*/
int migrateDiffCb(void *pArg, const ProllyDiffChange *pChange){
  struct MigrateDiffCtx *ctx = (struct MigrateDiffCtx*)pArg;
  const u8 *pVal;
  int nVal;
  int aType[64], aOffset[64];
  int nFields = 0;
  int sj, bindIdx;

  if( pChange->type == PROLLY_DIFF_DELETE ) return SQLITE_OK;

  pVal = pChange->pNewVal;
  nVal = pChange->nNewVal;
  if( !pVal || nVal<=0 ) return SQLITE_OK;

  /* Parse record header */
  {
    const u8 *hp = pVal;
    const u8 *hpEnd = pVal + nVal;
    u64 hdrSize;
    int hdrBytes, off;

    hdrBytes = dlReadVarint(hp, hpEnd, &hdrSize);
    hp += hdrBytes;
    off = (int)hdrSize;

    while( hp < pVal+hdrSize && hp < hpEnd && nFields<64 ){
      u64 st;
      int stBytes = dlReadVarint(hp, pVal+hdrSize, &st);
      hp += stBytes;
      aType[nFields] = (int)st;
      aOffset[nFields] = off;
      off += dlSerialTypeLen(st);
      nFields++;
    }
  }

  sqlite3_reset(ctx->pUpd);
  bindIdx = 1;
  for(sj=0; sj<ctx->nCols; sj++){
    if( ctx->aiColIdx[sj]<0 || !ctx->azColNames[sj] ) continue;
    if( ctx->aiColIdx[sj] < nFields ){
      int brc = doltliteBindField(ctx->pUpd, bindIdx, pVal, nVal,
                                  aType[ctx->aiColIdx[sj]],
                                  aOffset[ctx->aiColIdx[sj]]);
      if( brc!=SQLITE_OK ) return brc;
    }else{
      int brc = sqlite3_bind_null(ctx->pUpd, bindIdx);
      if( brc!=SQLITE_OK ) return brc;
    }
    bindIdx++;
  }
  {
    int brc = sqlite3_bind_int64(ctx->pUpd, bindIdx, pChange->intKey);
    int src;
    if( brc!=SQLITE_OK ) return brc;
    src = sqlite3_step(ctx->pUpd);
    if( src!=SQLITE_DONE ) return src;
  }

  return SQLITE_OK;
}

/*
** Migrate row data for added columns after a schema merge.
**
** After ALTER TABLE ADD COLUMN has been applied for theirs' columns, the
** merged table has the right schema but all NULLs for the new columns.
** This function diffs ancestor vs theirs and UPDATEs only changed rows
** to fill in the actual values for the added columns.
*/
int migrateSchemaRowData(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  const ProllyHash *pTheirCatHash,
  SchemaMergeAction *aActions,
  int nActions
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  struct TableEntry *aTheirTables = 0;
  int nTheirTables = 0;
  SchemaEntry *aTheirSchema = 0;
  int nTheirSchema = 0;
  int rc = SQLITE_OK;
  int si;

  struct TableEntry *aAncTables = 0;
  int nAncTables = 0;

  if( !cs || !pCache || nActions<=0 ) return SQLITE_OK;

  /* Load ancestor and theirs' catalogs to find table root hashes */
  rc = doltliteLoadCatalog(db, pAncCatHash, &aAncTables, &nAncTables, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, pTheirCatHash, &aTheirTables, &nTheirTables, 0);
  if( rc!=SQLITE_OK ){ sqlite3_free(aAncTables); return rc; }

  /* Load theirs' schema entries to find column ordinal positions */
  rc = loadSchemaFromCatalog(db, cs, pCache, pTheirCatHash,
                              &aTheirSchema, &nTheirSchema);
  if( rc!=SQLITE_OK ){
    sqlite3_free(aTheirTables);
    return rc;
  }

  for(si=0; si<nActions && rc==SQLITE_OK; si++){
    SchemaMergeAction *pAct = &aActions[si];
    struct TableEntry *theirTE;
    SchemaEntry *theirSE;
    char **azColNames = 0;   /* Column names extracted from ADD defs */
    int *aiColIdx = 0;       /* Column ordinal in theirs' schema */
    int nCols = 0;
    int sj;

    if( pAct->nAddColumns<=0 ) continue;

    /* Find theirs' table entry */
    theirTE = doltliteFindTableByName(aTheirTables, nTheirTables,
                                       pAct->zTableName);
    if( !theirTE ) continue;

    /* Find theirs' schema SQL to determine column order */
    theirSE = findSchemaEntry(aTheirSchema, nTheirSchema, pAct->zTableName);
    if( !theirSE || !theirSE->zSql ) continue;

    /* Parse theirs' schema to get column positions */
    {
      DoltliteColInfo theirCols;
      memset(&theirCols, 0, sizeof(theirCols));

      /* We need to parse theirs' CREATE TABLE to find column ordinals.
      ** Use PRAGMA-style parsing via a temporary approach: parse the SQL
      ** to count columns and find positions. */

      /* First, extract column names from the ADD COLUMN definitions */
      azColNames = sqlite3_malloc(pAct->nAddColumns * (int)sizeof(char*));
      aiColIdx = sqlite3_malloc(pAct->nAddColumns * (int)sizeof(int));
      if( !azColNames || !aiColIdx ){
        sqlite3_free(azColNames);
        sqlite3_free(aiColIdx);
        rc = SQLITE_NOMEM;
        break;
      }
      memset(azColNames, 0, pAct->nAddColumns * (int)sizeof(char*));
      nCols = 0;

      for(sj=0; sj<pAct->nAddColumns; sj++){
        azColNames[sj] = extractColNameFromDef(pAct->azAddColumns[sj]);
        if( !azColNames[sj] ){
          rc = SQLITE_NOMEM;
          break;
        }
        aiColIdx[sj] = -1;
      }
      if( rc!=SQLITE_OK ) break;

      /* Now parse theirs' CREATE TABLE SQL to find each column's ordinal.
      ** Column ordinal in the record = position in CREATE TABLE column list.
      ** For INTEGER PRIMARY KEY tables, the PK column is NOT stored in the
      ** record (it's the rowid/intKey), so the ordinal needs adjustment. */
      {
        const char *zSql = theirSE->zSql;
        const char *p = zSql;
        int colOrdinal = 0;
        int depth = 0;
        const char *segStart;

        /* Find '(' */
        while( *p && *p!='(' ) p++;
        if( !*p ){ rc = SQLITE_CORRUPT; goto next_action; }
        p++;

        segStart = p;
        depth = 0;
        {
          const char *pSqlEnd = p;
          int d2 = 1;
          while( *pSqlEnd && d2>0 ){
            if(*pSqlEnd=='(') d2++;
            else if(*pSqlEnd==')') d2--;
            pSqlEnd++;
          }
          if( d2!=0 ){ rc = SQLITE_CORRUPT; goto next_action; }
          pSqlEnd--; /* back to ')' */

          while( p <= pSqlEnd ){
            if( p==pSqlEnd || (*p==',' && depth==0) ){
              /* Extract column name from this segment */
              const char *s = segStart;
              const char *e = p;
              int isConstraint = 0;

              while( s<e && isspace((unsigned char)*s) ) s++;
              while( e>s && isspace((unsigned char)*(e-1)) ) e--;

              if( e > s ){
                int segLen = (int)(e - s);
                /* Check if it's a constraint, not a column */
                if( segLen>=11 && sqlite3_strnicmp(s, "PRIMARY KEY", 11)==0
                    && (segLen==11 || !isalnum((unsigned char)s[11])) ){
                  isConstraint = 1;
                }else if( segLen>=6 && sqlite3_strnicmp(s, "UNIQUE", 6)==0
                    && (segLen==6 || s[6]=='(' || isspace((unsigned char)s[6])) ){
                  isConstraint = 1;
                }else if( segLen>=5 && sqlite3_strnicmp(s, "CHECK", 5)==0
                    && (segLen==5 || s[5]=='(' || isspace((unsigned char)s[5])) ){
                  isConstraint = 1;
                }else if( segLen>=11 && sqlite3_strnicmp(s, "FOREIGN KEY", 11)==0
                    && (segLen==11 || !isalnum((unsigned char)s[11])) ){
                  isConstraint = 1;
                }else if( segLen>=10 && sqlite3_strnicmp(s, "CONSTRAINT", 10)==0
                    && (segLen==10 || isspace((unsigned char)s[10])) ){
                  isConstraint = 1;
                }

                if( !isConstraint ){
                  /* Extract the column name */
                  char *zColName;
                  const char *ns = s;
                  int nl = dlIdentTokenLen(ns, (int)(e - ns));

                  zColName = dlExtractIdentLower(ns, nl);
                  if( !zColName ){
                    rc = SQLITE_NOMEM;
                    goto next_action;
                  }
                  /* Check if this column matches any of our added columns */
                  for(sj=0; sj<pAct->nAddColumns; sj++){
                    if( azColNames[sj]
                     && strcmp(azColNames[sj], zColName)==0 ){
                      aiColIdx[sj] = colOrdinal;
                    }
                  }
                  sqlite3_free(zColName);
                  if( rc!=SQLITE_OK ){
                    goto next_action;
                  }
                  colOrdinal++;
                }
              }
              segStart = p + 1;
            }else if( *p=='(' ){
              depth++;
            }else if( *p==')' ){
              depth--;
            }
            p++;
          }
        }
      }

      nCols = pAct->nAddColumns;

      /* Check that we found at least one column to migrate */
      {
        int hasAny = 0;
        for(sj=0; sj<nCols; sj++){
          if( aiColIdx[sj]>=0 ){ hasAny = 1; break; }
        }
        if( !hasAny ) goto next_action;
      }

      /* Build the UPDATE statement:
      ** UPDATE "<table>" SET col1=?1, col2=?2, ... WHERE rowid=?N */
      {
        char *zUpdate;
        char *zSet = 0;
        int paramIdx = 1;
        sqlite3_stmt *pUpd = 0;

        for(sj=0; sj<nCols; sj++){
          if( aiColIdx[sj]<0 || !azColNames[sj] ) continue;
          if( zSet ){
            char *zNew = sqlite3_mprintf("%s, \"%w\"=?%d",
                                          zSet, azColNames[sj], paramIdx);
            sqlite3_free(zSet);
            zSet = zNew;
          }else{
            zSet = sqlite3_mprintf("\"%w\"=?%d", azColNames[sj], paramIdx);
          }
          paramIdx++;
        }

        if( !zSet ) goto next_action;

        zUpdate = sqlite3_mprintf("UPDATE \"%w\" SET %s WHERE rowid=?%d",
                                   pAct->zTableName, zSet, paramIdx);
        sqlite3_free(zSet);
        if( !zUpdate ){
          rc = SQLITE_NOMEM;
          goto next_action;
        }

        rc = sqlite3_prepare_v2(db, zUpdate, -1, &pUpd, 0);
        sqlite3_free(zUpdate);
        if( rc!=SQLITE_OK ) goto next_action;

        /* Diff ancestor vs theirs -- only UPDATE rows that changed */
        {
          struct TableEntry *ancTE;
          ProllyHash ancRoot;
          struct MigrateDiffCtx diffCtx;

          ancTE = doltliteFindTableByName(aAncTables, nAncTables,
                                           pAct->zTableName);
          if( ancTE ){
            memcpy(&ancRoot, &ancTE->root, sizeof(ProllyHash));
          }else{
            memset(&ancRoot, 0, sizeof(ancRoot));
          }

          diffCtx.pUpd = pUpd;
          diffCtx.aiColIdx = aiColIdx;
          diffCtx.azColNames = azColNames;
          diffCtx.nCols = nCols;

          rc = prollyDiff(cs, pCache, &ancRoot, &theirTE->root,
                          theirTE->flags, migrateDiffCb, &diffCtx);
        }

        sqlite3_finalize(pUpd);
      }
    }

next_action:
    /* Free column name array */
    if( azColNames ){
      for(sj=0; sj<pAct->nAddColumns; sj++) sqlite3_free(azColNames[sj]);
      sqlite3_free(azColNames);
    }
    sqlite3_free(aiColIdx);

    if( rc!=SQLITE_OK ) break;
  }

  freeSchemaEntries(aTheirSchema, nTheirSchema);
  sqlite3_free(aTheirTables);
  sqlite3_free(aAncTables);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
