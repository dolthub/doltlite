/*
** Schema merge helpers. Extracted from doltlite.c.
**
** Functions that handle data migration after a schema merge:
**   extractColNameFromDef()  - parse column name from ADD COLUMN def
**   bindRecordField()        - bind a field from a SQLite record blob
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

/*
** Extract the column name from an ADD COLUMN definition string like "y INTEGER".
** Returns a malloc'd, lowercased copy of the name. Caller must sqlite3_free.
*/
char *extractColNameFromDef(const char *zDef){
  const char *s = zDef;
  const char *e;
  char *zName;
  int len, i;

  /* Skip leading whitespace */
  while( *s && isspace((unsigned char)*s) ) s++;
  if( !*s ) return 0;

  /* Handle quoted names */
  if( *s=='"' || *s=='`' || *s=='[' ){
    char close = (*s=='"') ? '"' : (*s=='`') ? '`' : ']';
    s++;
    e = s;
    while( *e && *e!=close ) e++;
    len = (int)(e - s);
  }else{
    e = s;
    while( *e && !isspace((unsigned char)*e) && *e!='(' && *e!=',' ) e++;
    len = (int)(e - s);
  }
  if( len<=0 ) return 0;
  zName = sqlite3_malloc(len + 1);
  if( !zName ) return 0;
  memcpy(zName, s, len);
  zName[len] = 0;
  for(i=0; zName[i]; i++) zName[i] = (char)tolower((unsigned char)zName[i]);
  return zName;
}

/*
** Bind a field from a SQLite record-format blob to a prepared statement
** parameter. The field is identified by its serial type and offset within
** the record data.
*/
int bindRecordField(
  sqlite3_stmt *pStmt,
  int iParam,             /* 1-based bind parameter index */
  const u8 *pData,
  int nData,
  int serialType,
  int offset
){
  if( serialType==0 ){
    return sqlite3_bind_null(pStmt, iParam);
  }
  if( serialType==8 ){
    return sqlite3_bind_int(pStmt, iParam, 0);
  }
  if( serialType==9 ){
    return sqlite3_bind_int(pStmt, iParam, 1);
  }
  if( serialType>=1 && serialType<=6 ){
    static const int sz[] = {0,1,2,3,4,6,8};
    int nB = sz[serialType];
    if( offset+nB <= nData ){
      const u8 *q = pData + offset;
      i64 v = (q[0] & 0x80) ? -1 : 0;
      int i;
      for(i=0; i<nB; i++) v = (v<<8) | q[i];
      return sqlite3_bind_int64(pStmt, iParam, v);
    }
    return sqlite3_bind_null(pStmt, iParam);
  }
  if( serialType==7 ){
    if( offset+8 <= nData ){
      const u8 *q = pData + offset;
      double v;
      u64 bits = 0;
      int i;
      for(i=0; i<8; i++) bits = (bits<<8) | q[i];
      memcpy(&v, &bits, 8);
      return sqlite3_bind_double(pStmt, iParam, v);
    }
    return sqlite3_bind_null(pStmt, iParam);
  }
  if( serialType>=13 && (serialType&1)==1 ){
    int len = (serialType-13)/2;
    if( offset+len <= nData ){
      return sqlite3_bind_text(pStmt, iParam,
                               (const char*)(pData+offset), len, SQLITE_TRANSIENT);
    }
    return sqlite3_bind_null(pStmt, iParam);
  }
  if( serialType>=12 && (serialType&1)==0 ){
    int len = (serialType-12)/2;
    if( offset+len <= nData ){
      return sqlite3_bind_blob(pStmt, iParam, pData+offset, len, SQLITE_TRANSIENT);
    }
    return sqlite3_bind_null(pStmt, iParam);
  }
  return sqlite3_bind_null(pStmt, iParam);
}

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
      bindRecordField(ctx->pUpd, bindIdx, pVal, nVal,
                      aType[ctx->aiColIdx[sj]],
                      aOffset[ctx->aiColIdx[sj]]);
    }else{
      sqlite3_bind_null(ctx->pUpd, bindIdx);
    }
    bindIdx++;
  }
  sqlite3_bind_int64(ctx->pUpd, bindIdx, pChange->intKey);
  sqlite3_step(ctx->pUpd);

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
        aiColIdx[sj] = -1;
      }

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
        if( !*p ) goto next_action;
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
          if( d2!=0 ) goto next_action;
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
                  const char *ne;
                  int nl;

                  if( *ns=='"' || *ns=='`' || *ns=='[' ){
                    char close = (*ns=='"') ? '"' : (*ns=='`') ? '`' : ']';
                    ns++;
                    ne = ns;
                    while( ne<e && *ne!=close ) ne++;
                    nl = (int)(ne - ns);
                  }else{
                    ne = ns;
                    while( ne<e && !isspace((unsigned char)*ne)
                        && *ne!='(' && *ne!=',' ) ne++;
                    nl = (int)(ne - ns);
                  }

                  zColName = sqlite3_malloc(nl + 1);
                  if( zColName ){
                    int ci;
                    memcpy(zColName, ns, nl);
                    zColName[nl] = 0;
                    for(ci=0; zColName[ci]; ci++)
                      zColName[ci] = (char)tolower((unsigned char)zColName[ci]);

                    /* Check if this column matches any of our added columns */
                    for(sj=0; sj<pAct->nAddColumns; sj++){
                      if( azColNames[sj]
                       && strcmp(azColNames[sj], zColName)==0 ){
                        aiColIdx[sj] = colOrdinal;
                      }
                    }
                    sqlite3_free(zColName);
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
          if( rc!=SQLITE_OK ) rc = SQLITE_OK; /* best-effort */
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

    /* Continue even on non-fatal errors */
    if( rc!=SQLITE_OK && rc!=SQLITE_NOMEM ) rc = SQLITE_OK;
  }

  freeSchemaEntries(aTheirSchema, nTheirSchema);
  sqlite3_free(aTheirTables);
  sqlite3_free(aAncTables);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
