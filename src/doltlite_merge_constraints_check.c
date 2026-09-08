#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"

static int checkIsIdent(char c){
  return sqlite3Isalnum(c) || c=='_';
}

static int nextCheckClause(
  const char *zSql, int *pOffset, char **pzExpr, char **pzName
){
  const char *p = zSql + *pOffset;
  const char *pEnd;
  char lastConstraintName[128] = {0};
  int depth;
  const char *pExprStart;

  *pzExpr = 0;
  *pzName = 0;

  while( *p ){
    if( (p==zSql || !checkIsIdent(p[-1]))
     && (p[0]=='C' || p[0]=='c')
     && sqlite3_strnicmp(p, "CONSTRAINT", 10)==0
     && (p[10]==' ' || p[10]=='\t' || p[10]=='\n') ){
      int i = 0;
      p += 10;
      while( *p==' ' || *p=='\t' || *p=='\n' ) p++;
      while( *p && *p!=' ' && *p!='\t' && *p!='\n' && *p!='(' && i<127 ){
        lastConstraintName[i++] = *p++;
      }
      lastConstraintName[i] = 0;
      continue;
    }
    if( (p==zSql || !checkIsIdent(p[-1]))
     && (p[0]=='C' || p[0]=='c')
     && sqlite3_strnicmp(p, "CHECK", 5)==0
     && (p[5]==' ' || p[5]=='\t' || p[5]=='(' || p[5]=='\n') ){
      p += 5;
      while( *p==' ' || *p=='\t' || *p=='\n' ) p++;
      if( *p!='(' ){ p++; lastConstraintName[0] = 0; continue; }
      p++;
      pExprStart = p;
      depth = 1;
      while( *p && depth>0 ){
        char c = *p;
        if( c=='\'' ){
          p++;
          while( *p && !(*p=='\'' && p[1]!='\'') ){
            if( *p=='\'' && p[1]=='\'' ) p++;
            p++;
          }
          if( *p=='\'' ) p++;
          continue;
        }
        if( c=='"' ){
          p++;
          while( *p && *p!='"' ) p++;
          if( *p=='"' ) p++;
          continue;
        }
        if( c=='(' ) depth++;
        else if( c==')' ) depth--;
        if( depth>0 ) p++;
      }
      if( depth!=0 ) return -SQLITE_CORRUPT;
      pEnd = p;
      p++;
      *pzExpr = sqlite3_malloc((int)(pEnd - pExprStart) + 1);
      if( !*pzExpr ) return -SQLITE_NOMEM;
      memcpy(*pzExpr, pExprStart, (size_t)(pEnd - pExprStart));
      (*pzExpr)[pEnd - pExprStart] = 0;
      if( lastConstraintName[0] ){
        *pzName = sqlite3_mprintf("%s", lastConstraintName);
        if( !*pzName ){
          sqlite3_free(*pzExpr);
          *pzExpr = 0;
          return -SQLITE_NOMEM;
        }
      }
      *pOffset = (int)(p - zSql);
      return 1;
    }
    if( *p=='\'' ){
      p++;
      while( *p && !(*p=='\'' && p[1]!='\'') ){
        if( *p=='\'' && p[1]=='\'' ) p++;
        p++;
      }
      if( *p=='\'' ) p++;
      continue;
    }
    if( *p=='"' ){
      p++;
      while( *p && *p!='"' ) p++;
      if( *p=='"' ) p++;
      continue;
    }
    if( *p==',' ) lastConstraintName[0] = 0;
    p++;
  }
  *pOffset = (int)(p - zSql);
  return 0;
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

      zInfo = sqlite3_mprintf(
          "{\"Name\": \"%w\", \"Expression\": \"%w\"}",
          zCkName ? zCkName : "", zExpr);
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
