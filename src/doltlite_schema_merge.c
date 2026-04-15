
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

char *extractColNameFromDef(const char *zDef){
  const char *s = zDef;
  int len;


  while( *s && isspace((unsigned char)*s) ) s++;
  if( !*s ) return 0;

  len = dlIdentTokenLen(s, (int)strlen(s));
  return dlExtractIdentLower(s, len);
}

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

/* After schema merge decides to ADD COLUMN X from theirs, every
** row that exists on theirs needs X backfilled onto ours. Parse
** their CREATE TABLE to find each new column's ordinal, then diff
** their table against anc and UPDATE each row on ours with the
** value of the new column. Rows that only exist on theirs (diff
** ADD) also need the update. Rows DELETEd on theirs are skipped
** (trySchemaColumnMerge has already rejected drop-on-one-side
** edit-on-other, so we shouldn't see that case here). */
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


  rc = doltliteLoadCatalog(db, pAncCatHash, &aAncTables, &nAncTables, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, pTheirCatHash, &aTheirTables, &nTheirTables, 0);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(aAncTables, nAncTables);
    return rc;
  }


  rc = loadSchemaFromCatalog(db, cs, pCache, pTheirCatHash,
                              &aTheirSchema, &nTheirSchema);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(aAncTables, nAncTables);
    doltliteFreeCatalog(aTheirTables, nTheirTables);
    return rc;
  }

  for(si=0; si<nActions && rc==SQLITE_OK; si++){
    SchemaMergeAction *pAct = &aActions[si];
    struct TableEntry *theirTE;
    SchemaEntry *theirSE;
    char **azColNames = 0;
    int *aiColIdx = 0;
    int nCols = 0;
    int sj;

    if( pAct->nAddColumns<=0 ) continue;


    theirTE = doltliteFindTableByName(aTheirTables, nTheirTables,
                                       pAct->zTableName);
    if( !theirTE ) continue;


    theirSE = findSchemaEntry(aTheirSchema, nTheirSchema, pAct->zTableName);
    if( !theirSE || !theirSE->zSql ) continue;


    {
      DoltliteColInfo theirCols;
      memset(&theirCols, 0, sizeof(theirCols));




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


      {
        const char *zSql = theirSE->zSql;
        const char *p = zSql;
        int colOrdinal = 0;
        int depth = 0;
        const char *segStart;


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
          pSqlEnd--;

          while( p <= pSqlEnd ){
            if( p==pSqlEnd || (*p==',' && depth==0) ){

              const char *s = segStart;
              const char *e = p;
              int isConstraint = 0;

              while( s<e && isspace((unsigned char)*s) ) s++;
              while( e>s && isspace((unsigned char)*(e-1)) ) e--;

              if( e > s ){
                int segLen = (int)(e - s);

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

                  char *zColName;
                  const char *ns = s;
                  int nl = dlIdentTokenLen(ns, (int)(e - ns));

                  zColName = dlExtractIdentLower(ns, nl);
                  if( !zColName ){
                    rc = SQLITE_NOMEM;
                    goto next_action;
                  }

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


      {
        int hasAny = 0;
        for(sj=0; sj<nCols; sj++){
          if( aiColIdx[sj]>=0 ){ hasAny = 1; break; }
        }
        if( !hasAny ) goto next_action;
      }


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

    if( azColNames ){
      for(sj=0; sj<pAct->nAddColumns; sj++) sqlite3_free(azColNames[sj]);
      sqlite3_free(azColNames);
    }
    sqlite3_free(aiColIdx);

    if( rc!=SQLITE_OK ) break;
  }

  freeSchemaEntries(aTheirSchema, nTheirSchema);
  doltliteFreeCatalog(aTheirTables, nTheirTables);
  doltliteFreeCatalog(aAncTables, nAncTables);
  return rc;
}

#endif
