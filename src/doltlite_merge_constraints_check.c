#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"

static int nextCheckToken(const char **pzSql, int *pType){
  int n;
  do{
    if( !**pzSql ) return 0;
    n = sqlite3GetToken((const u8*)*pzSql, pType);
    if( n<=0 || *pType==TK_ILLEGAL ) return -SQLITE_CORRUPT;
    if( *pType!=TK_SPACE && *pType!=TK_COMMENT ) return n;
    *pzSql += n;
  }while( 1 );
}

static int nextCheckClause(
  const char *zSql, int *pOffset, char **pzExpr, char **pzName
){
  const char *p = zSql + *pOffset;
  const char *zName = 0;
  int nName = 0;
  int type, n;

  *pzExpr = 0;
  *pzName = 0;

  while( (n = nextCheckToken(&p, &type))>0 ){
    p += n;
    if( type==TK_CONSTRAINT ){
      n = nextCheckToken(&p, &type);
      if( n<=0 ) return n<0 ? n : -SQLITE_CORRUPT;
      zName = p;
      nName = n;
      p += n;
      continue;
    }
    if( type==TK_CHECK ){
      const char *pExprStart;
      const char *pEnd;
      int depth = 1;
      n = nextCheckToken(&p, &type);
      if( n<=0 || type!=TK_LP ) return -SQLITE_CORRUPT;
      p += n;
      pExprStart = p;
      while( depth>0 ){
        n = nextCheckToken(&p, &type);
        if( n<=0 ) return n<0 ? n : -SQLITE_CORRUPT;
        if( type==TK_LP ) depth++;
        if( type==TK_RP ) depth--;
        pEnd = p;
        p += n;
      }
      *pzExpr = sqlite3_mprintf("%.*s", (int)(pEnd-pExprStart), pExprStart);
      if( !*pzExpr ) return -SQLITE_NOMEM;
      if( zName ){
        *pzName = sqlite3_mprintf("%.*s", nName, zName);
        if( !*pzName ){
          sqlite3_free(*pzExpr);
          *pzExpr = 0;
          return -SQLITE_NOMEM;
        }
        sqlite3Dequote(*pzName);
      }
      *pOffset = (int)(p - zSql);
      return 1;
    }
    zName = 0;
  }
  *pOffset = (int)(p - zSql);
  return n;
}

static void appendCheckJsonString(sqlite3_str *pJson, const char *z){
  sqlite3_str_appendchar(pJson, 1, '"');
  for(; *z; z++){
    unsigned char c = (unsigned char)*z;
    if( c=='"' || c=='\\' ){
      sqlite3_str_appendchar(pJson, 1, '\\');
      sqlite3_str_appendchar(pJson, 1, c);
    }else if( c<0x20 ){
      sqlite3_str_appendf(pJson, "\\u%04x", c);
    }else{
      sqlite3_str_appendchar(pJson, 1, c);
    }
  }
  sqlite3_str_appendchar(pJson, 1, '"');
}

typedef struct CheckWalk CheckWalk;
struct CheckWalk {
  char **pzErrMsg;
  int *pnFound;
};

static int checkWalkTable(
  sqlite3 *db,
  const char *zTable,
  const char *zSql,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aCur, int nCur,
  void *pCtx
){
  CheckWalk *pWalk = (CheckWalk*)pCtx;
  int offset = 0;
  int hasRowid = 1;
  MergePkInfo pkInfo;
  int rc;
  (void)aCur; (void)nCur;

  memset(&pkInfo, 0, sizeof(pkInfo));
  rc = tableHasRowid(db, zTable, &hasRowid);
  if( rc!=SQLITE_OK ) return rc;
  if( !hasRowid ){
    rc = loadMergePkInfo(db, zTable, &pkInfo);
    if( rc != SQLITE_OK ) return rc;
  }

  for(;;){
    char *zExpr = 0;
    char *zCkName = 0;
    int clauseRc = nextCheckClause(zSql, &offset, &zExpr, &zCkName);
    char *zQuery;
    sqlite3_stmt *pQ = 0;
    int queryStepRc;

    if( clauseRc <= 0 ){
      sqlite3_free(zExpr);
      sqlite3_free(zCkName);
      if( clauseRc<0 ) rc = -clauseRc;
      break;
    }

    if( hasRowid ){
      zQuery = sqlite3_mprintf(
          "SELECT rowid FROM main.\"%w\" NOT INDEXED WHERE NOT (%s)",
          zTable, zExpr);
    }else{
      zQuery = sqlite3_mprintf(
          "SELECT %s FROM main.\"%w\" NOT INDEXED WHERE NOT (%s)",
          pkInfo.zPkCols, zTable, zExpr);
    }
    if( !zQuery ){
      sqlite3_free(zExpr);
      sqlite3_free(zCkName);
      rc = SQLITE_NOMEM;
      break;
    }
    rc = sqlite3_prepare_v2(db, zQuery, -1, &pQ, 0);
    sqlite3_free(zQuery);
    if( rc != SQLITE_OK ){
      setConstraintError(db, pWalk->pzErrMsg, rc);
      sqlite3_free(zExpr);
      sqlite3_free(zCkName);
      break;
    }

    while( (queryStepRc = sqlite3_step(pQ)) == SQLITE_ROW ){
      u8 *pKey = 0; int nKey = 0;
      u8 *pVal = 0; int nVal = 0;
      char *zInfo;
      sqlite3_str *pJson;
      int appendRc;
      i64 intKey = 0;

      if( hasRowid ){
        intKey = sqlite3_column_int64(pQ, 0);
        rc = fetchOrphanRow(db, zTable, intKey, &pKey, &nKey, &pVal, &nVal);
      }else{
        u8 *pPkRec = 0; int nPkRec = 0;
        pPkRec = buildRecordFromStmtCols(pQ, 0, pkInfo.nPk, &nPkRec);
        if( !pPkRec ){ rc = SQLITE_NOMEM; break; }
        rc = fetchRowByPkFromTable(db, zTable, pPkRec, nPkRec, pkInfo.nPk,
                                   &pKey, &nKey, &pVal, &nVal);
        sqlite3_free(pPkRec);
      }
      if( rc == SQLITE_NOTFOUND ){ rc = SQLITE_OK; continue; }
      if( rc != SQLITE_OK ){
        sqlite3_free(pKey);
        sqlite3_free(pVal);
        break;
      }

      if( aAnc ){
        u8 *pAncVal = 0; int nAncVal = 0;
        int ancRc = hasRowid
            ? fetchAncestorRowByName(db, aAnc, nAnc, zTable,
                                     intKey, &pAncVal, &nAncVal)
            : fetchAncestorRowByKey(db, aAnc, nAnc, zTable,
                                    pKey, nKey, &pAncVal, &nAncVal);
        int preExisting = (ancRc==SQLITE_OK)
            && isRowPreExisting(pVal, nVal, pAncVal, nAncVal);
        sqlite3_free(pAncVal);
        if( preExisting ){
          sqlite3_free(pKey);
          sqlite3_free(pVal);
          continue;
        }
      }

      pJson = sqlite3_str_new(db);
      sqlite3_str_appendall(pJson, "{\"Name\": ");
      appendCheckJsonString(pJson, zCkName ? zCkName : "");
      sqlite3_str_appendall(pJson, ", \"Expression\": ");
      appendCheckJsonString(pJson, zExpr);
      sqlite3_str_appendchar(pJson, 1, '}');
      zInfo = sqlite3_str_finish(pJson);
      if( !zInfo ){
        sqlite3_free(pKey);
        sqlite3_free(pVal);
        rc = SQLITE_NOMEM;
        break;
      }
      appendRc = doltliteAppendConstraintViolation(
          db, zTable, DOLTLITE_CV_CHECK_CONSTRAINT,
          intKey, pKey, nKey, pVal, nVal, zInfo);
      sqlite3_free(zInfo);
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      if( appendRc != SQLITE_OK ){ rc = appendRc; break; }
      if( pWalk->pnFound ) (*pWalk->pnFound)++;
    }
    if( rc==SQLITE_OK && queryStepRc!=SQLITE_DONE ) rc = queryStepRc;
    setConstraintError(db, pWalk->pzErrMsg, rc);
    rc = finishConstraintStmt(pQ, rc);
    setConstraintError(db, pWalk->pzErrMsg, rc);
    sqlite3_free(zExpr);
    sqlite3_free(zCkName);
    if( rc != SQLITE_OK ) break;
  }

  freeMergePkInfo(&pkInfo);
  return rc;
}

int doltliteDetectMergeCheckViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  char **pzErrMsg,
  int *pnFound,
  const char **azTables,
  int nTables
){
  CheckWalk walk;
  if( pnFound ) *pnFound = 0;
  walk.pzErrMsg = pzErrMsg;
  walk.pnFound = pnFound;
  return walkMergeUserTables(db, pAncCatHash, pzErrMsg, azTables, nTables,
                             1, 1, checkWalkTable, &walk);
}


#endif
