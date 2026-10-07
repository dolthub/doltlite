#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_constraints_int.h"
#include "doltlite_merge_int.h"
#include <ctype.h>

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

static int checkWalkTable(
  sqlite3 *db,
  const char *zTable,
  const char *zSql,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aCur, int nCur,
  void *pCtx
){
  MergeConstraintWalk *pWalk = (MergeConstraintWalk*)pCtx;
  int offset = 0;
  int hasRowid = 1;
  MergePkInfo pkInfo;
  char *zRowid = 0;
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
      if( !zRowid ){
        rc = loadMergeRowidSql(db, zTable, &zRowid, pWalk->pzErrMsg);
      }
      if( rc!=SQLITE_OK ){
        sqlite3_free(zExpr);
        sqlite3_free(zCkName);
        break;
      }
      zQuery = sqlite3_mprintf(
          "SELECT %s FROM main.\"%w\" NOT INDEXED WHERE NOT (%s)",
          zRowid, zTable, zExpr);
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
  sqlite3_free(zRowid);
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
  MergeConstraintWalk walk;
  if( pnFound ) *pnFound = 0;
  walk.pzErrMsg = pzErrMsg;
  walk.pnFound = pnFound;
  return walkMergeUserTables(db, pAncCatHash, pzErrMsg, azTables, nTables,
                             1, 1, checkWalkTable, &walk);
}

/* A CHECK clause. zCols is newline-separated referenced column names. */
typedef struct DlCheck DlCheck;
struct DlCheck {
  char *zName;
  char *zExpr;
  char *zRaw;
  char *zCols;
  int iOffset;
};

static void dlChecksFree(DlCheck *a, int n){
  int i;
  if( !a ) return;
  for(i=0; i<n; i++){
    sqlite3_free(a[i].zName);
    sqlite3_free(a[i].zExpr);
    sqlite3_free(a[i].zRaw);
    sqlite3_free(a[i].zCols);
  }
  sqlite3_free(a);
}

int dlColsContain(const char *zCols, const char *zName){
  int n;
  const char *p;
  if( !zCols || !zName ) return 0;
  n = (int)strlen(zName);
  for(p=zCols; *p; ){
    const char *e = strchr(p, '\n');
    int m = e ? (int)(e-p) : (int)strlen(p);
    if( m==n && sqlite3_strnicmp(p, zName, n)==0 ) return 1;
    if( !e ) break;
    p = e+1;
  }
  return 0;
}

static int dlChecksOverlap(const DlCheck *pA, const DlCheck *pB){
  const char *p;
  if( !pA->zCols || !pB->zCols ) return 0;
  for(p=pA->zCols; *p; ){
    const char *e = strchr(p, '\n');
    int n = e ? (int)(e-p) : (int)strlen(p);
    const char *q;
    for(q=pB->zCols; *q; ){
      const char *f = strchr(q, '\n');
      int m = f ? (int)(f-q) : (int)strlen(q);
      if( n>0 && m==n && sqlite3_strnicmp(p, q, n)==0 ) return 1;
      if( !f ) break;
      q = f+1;
    }
    if( !e ) break;
    p = e+1;
  }
  return 0;
}

static int dlCheckAddCol(
  DlCheck *p,
  ParsedColumn *aCols,
  int nCols,
  const char *z
){
  char *zName;
  char *zNew;
  int i;
  zName = sqlite3_mprintf("%s", z);
  if( !zName ) return SQLITE_NOMEM;
  for(i=0; zName[i]; i++) zName[i] = (char)tolower((unsigned char)zName[i]);
  if( parsedColumnIndexByName(aCols, nCols, zName)<0 || dlColsContain(p->zCols, zName) ){
    sqlite3_free(zName);
    return SQLITE_OK;
  }
  zNew = sqlite3_mprintf("%s%s%s", p->zCols ? p->zCols : "", p->zCols ? "\n" : "", zName);
  sqlite3_free(zName);
  if( !zNew ) return SQLITE_NOMEM;
  sqlite3_free(p->zCols);
  p->zCols = zNew;
  return SQLITE_OK;
}

typedef struct DlCheckCols DlCheckCols;
struct DlCheckCols {
  Walker walker;
  DlCheck *pCheck;
  ParsedColumn *aCols;
  int nCols;
  int rc;
};

static int dlCheckColumnExpr(Walker *pWalker, Expr *pExpr){
  DlCheckCols *p = (DlCheckCols*)pWalker;
  int result = WRC_Continue;
  if( pExpr->op==TK_DOT ){
    pExpr = pExpr->pRight;
    result = WRC_Prune;
  }
  if( pExpr->op==TK_ID ){
    p->rc = dlCheckAddCol(p->pCheck, p->aCols, p->nCols, pExpr->u.zToken);
    if( p->rc!=SQLITE_OK ) return WRC_Abort;
  }
  return result;
}

static int dlCheckNoteCols(DlCheck *p, ParsedColumn *aCols, int nCols){
  sqlite3 *tmp = 0;
  char *zSql;
  int rc;
  /* A view retains the unresolved expression without evaluating functions. */
  zSql = sqlite3_mprintf("CREATE TEMP VIEW dl_check AS SELECT (%s\n)",
                        p->zExpr ? p->zExpr : "");
  if( !zSql ) return SQLITE_NOMEM;
  rc = sqlite3_open(":memory:", &tmp);
  if( rc==SQLITE_OK ) rc = sqlite3_exec(tmp, zSql, 0, 0, 0);
  if( rc==SQLITE_OK ){
    Table *pTab;
    DlCheckCols walk;
    memset(&walk, 0, sizeof(walk));
    walk.walker.xExprCallback = dlCheckColumnExpr;
    walk.pCheck = p;
    walk.aCols = aCols;
    walk.nCols = nCols;
    sqlite3_mutex_enter(tmp->mutex);
    pTab = sqlite3FindTable(tmp, "dl_check", "temp");
    if( !pTab || !IsView(pTab) || !pTab->u.view.pSelect ){
      rc = SQLITE_CORRUPT;
    }else{
      sqlite3WalkExprList(&walk.walker, pTab->u.view.pSelect->pEList);
      rc = walk.rc;
    }
    sqlite3_mutex_leave(tmp->mutex);
  }
  if( tmp ) sqlite3_close(tmp);
  sqlite3_free(zSql);
  return rc;
}

static int dlChecksSame(const DlCheck *pA, const DlCheck *pB){
  if( (pA->zName==0)!=(pB->zName==0) ) return 0;
  if( pA->zName && sqlite3_stricmp(pA->zName, pB->zName)!=0 ) return 0;
  return schemaDefinitionsEquivalent(pA->zExpr ? pA->zExpr : "",
                                     pB->zExpr ? pB->zExpr : "");
}

/* Next CHECK in zSql. 1 found, 0 done, negative sqlite rc on failure.
** A CONSTRAINT name applies only when CHECK is the next clause. */
static int nextDlCheck(
  const char *zSql,
  int *pOffset,
  DlCheck *pChk,
  ParsedColumn *aCols,
  int nCols
){
  const char *p = zSql + *pOffset;
  const char *zNameTok = 0;
  const char *zRaw = 0;
  int nName = 0;
  int type, n;

  memset(pChk, 0, sizeof(*pChk));
  while( (n = nextCheckToken(&p, &type))>0 ){
    const char *tok = p;
    p += n;
    if( type==TK_CONSTRAINT ){
      const char *zSave;
      int kt, kn, nt;
      kn = nextCheckToken(&p, &nt);
      if( kn<=0 ) return kn<0 ? kn : -SQLITE_CORRUPT;
      zNameTok = p;
      nName = kn;
      p += kn;
      zSave = p;
      kn = nextCheckToken(&p, &kt);
      if( kn<=0 ) return kn<0 ? kn : -SQLITE_CORRUPT;
      if( kt!=TK_CHECK ){
        zNameTok = 0;
        nName = 0;
        zRaw = 0;
        continue;
      }
      zRaw = tok;
      p = zSave;
      continue;
    }
    if( type==TK_CHECK ){
      const char *zStart = zRaw ? zRaw : tok;
      const char *zExpr;
      const char *zExprEnd;
      int depth = 1;
      int rc;
      n = nextCheckToken(&p, &type);
      if( n<=0 || type!=TK_LP ) return -SQLITE_CORRUPT;
      p += n;
      zExpr = p;
      zExprEnd = p;
      while( depth>0 ){
        n = nextCheckToken(&p, &type);
        if( n<=0 ) return n<0 ? n : -SQLITE_CORRUPT;
        if( type==TK_LP ) depth++;
        if( type==TK_RP ) depth--;
        zExprEnd = p;
        p += n;
      }
      pChk->zExpr = sqlite3_mprintf("%.*s", (int)(zExprEnd-zExpr), zExpr);
      pChk->zRaw = sqlite3_mprintf("%.*s", (int)(p-zStart), zStart);
      pChk->iOffset = (int)(zStart-zSql);
      if( !pChk->zExpr || !pChk->zRaw ) return -SQLITE_NOMEM;
      if( zNameTok ){
        int i;
        pChk->zName = sqlite3_mprintf("%.*s", nName, zNameTok);
        if( !pChk->zName ) return -SQLITE_NOMEM;
        sqlite3Dequote(pChk->zName);
        for(i=0; pChk->zName[i]; i++){
          pChk->zName[i] = (char)tolower((unsigned char)pChk->zName[i]);
        }
        if( !pChk->zName[0] ){
          sqlite3_free(pChk->zName);
          pChk->zName = 0;
        }
      }
      rc = dlCheckNoteCols(pChk, aCols, nCols);
      if( rc!=SQLITE_OK ) return -rc;
      *pOffset = (int)(p - zSql);
      return 1;
    }
    zNameTok = 0;
    nName = 0;
    zRaw = 0;
  }
  return n;
}

static int dlCollectChecks(
  const char *zSql,
  ParsedColumn *aCols,
  int nCols,
  DlCheck **pp,
  int *pn
){
  int offset = 0, nAlloc = 0, rc = SQLITE_OK;
  *pp = 0;
  *pn = 0;
  for(;;){
    DlCheck chk;
    int got = nextDlCheck(zSql, &offset, &chk, aCols, nCols);
    if( got==0 ) return SQLITE_OK;
    if( got<0 ){
      sqlite3_free(chk.zName);
      sqlite3_free(chk.zExpr);
      sqlite3_free(chk.zRaw);
      sqlite3_free(chk.zCols);
      dlChecksFree(*pp, *pn);
      *pp = 0;
      *pn = 0;
      return -got;
    }
    rc = DOLTLITE_GROW_ARRAY(pp, &nAlloc, *pn+1, 4);
    if( rc!=SQLITE_OK ){
      sqlite3_free(chk.zName);
      sqlite3_free(chk.zExpr);
      sqlite3_free(chk.zRaw);
      sqlite3_free(chk.zCols);
      dlChecksFree(*pp, *pn);
      *pp = 0;
      *pn = 0;
      return rc;
    }
    (*pp)[(*pn)++] = chk;
  }
}

static int dlCoverAncestor(
  DlCheck *aAnc, int nAnc,
  DlCheck *aSide, int nSide,
  u8 *aUsed
){
  int i, j;
  if( nSide ) memset(aUsed, 0, (size_t)nSide);
  for(i=0; i<nAnc; i++){
    for(j=0; j<nSide; j++){
      if( aUsed[j] ) continue;
      if( dlChecksSame(&aAnc[i], &aSide[j]) ){
        aUsed[j] = 1;
        break;
      }
    }
    if( j==nSide ) return 0;
  }
  return 1;
}

static int dlSpliceChecks(const char *zSql, DlCheck *a, int n, char **pzOut){
  const char *p = zSql;
  const char *zEnd = zSql + strlen(zSql);
  const char *zRp = 0;
  sqlite3_str *pAdd;
  char *zAdd;
  int depth = 0, i;
  *pzOut = 0;
  while( p<zEnd ){
    int type, nTok = sqlite3GetToken((const u8*)p, &type);
    if( nTok<=0 || type==TK_ILLEGAL ) return SQLITE_CORRUPT;
    if( type==TK_LP ){ depth = 1; p += nTok; break; }
    p += nTok;
  }
  while( p<zEnd && depth ){
    int type, nTok = sqlite3GetToken((const u8*)p, &type);
    if( nTok<=0 || type==TK_ILLEGAL ) return SQLITE_CORRUPT;
    if( type==TK_LP ) depth++;
    else if( type==TK_RP && --depth==0 ){ zRp = p; break; }
    p += nTok;
  }
  if( !zRp ) return SQLITE_CORRUPT;
  pAdd = sqlite3_str_new(0);
  if( !pAdd ) return SQLITE_NOMEM;
  for(i=0; i<n; i++){
    sqlite3_str_appendall(pAdd, ", ");
    sqlite3_str_appendall(pAdd, a[i].zRaw ? a[i].zRaw : "");
  }
  if( sqlite3_str_errcode(pAdd) ){
    sqlite3_str_finish(pAdd);
    return SQLITE_NOMEM;
  }
  zAdd = sqlite3_str_finish(pAdd);
  if( !zAdd ) return SQLITE_NOMEM;
  *pzOut = sqlite3_mprintf("%.*s%s%s", (int)(zRp-zSql), zSql, zAdd, zRp);
  sqlite3_free(zAdd);
  return *pzOut ? SQLITE_OK : SQLITE_NOMEM;
}

/* 1 when both sides only added CHECK constraints, every ancestor check is
** still on both sides, and the new checks do not share a column or a name.
** *pzMerged is our CREATE with their missing checks inserted, or NULL when
** ours already has them. Other schema differences leave *pbUnion at 0. */
static int dlComposeDisjointChecks(
  const char *zAnc,
  const char *zOurs,
  const char *zTheirs,
  char **pzMerged,
  int *pbUnion
){
  ParsedColumn *aCols = 0;
  DlCheck *aAnc = 0, *aOurs = 0, *aTheirs = 0, *aAdd = 0;
  u8 *aUseOurs = 0, *aUseTheirs = 0;
  int nCols = 0, nAnc = 0, nOurs = 0, nTheirs = 0, nAdd = 0, nAddAlloc = 0;
  int i, j, rc, bMatch = 0, bUnion = 0;

  if( pzMerged ) *pzMerged = 0;
  *pbUnion = 0;
  rc = parseColumns(zOurs, &aCols, &nCols);
  if( rc!=SQLITE_OK ) return rc==SQLITE_NOMEM ? rc : SQLITE_OK;
  rc = schemaNonCheckTextMatches(zAnc, zOurs, &bMatch);
  if( rc==SQLITE_OK && bMatch ) rc = schemaNonCheckTextMatches(zAnc, zTheirs, &bMatch);
  if( rc!=SQLITE_OK || !bMatch ) goto done;
  rc = dlCollectChecks(zAnc, aCols, nCols, &aAnc, &nAnc);
  if( rc==SQLITE_OK ) rc = dlCollectChecks(zOurs, aCols, nCols, &aOurs, &nOurs);
  if( rc==SQLITE_OK ) rc = dlCollectChecks(zTheirs, aCols, nCols, &aTheirs, &nTheirs);
  if( rc!=SQLITE_OK ){
    if( rc!=SQLITE_NOMEM ) rc = SQLITE_OK;
    goto done;
  }
  aUseOurs = sqlite3_malloc(nOurs ? nOurs : 1);
  aUseTheirs = sqlite3_malloc(nTheirs ? nTheirs : 1);
  if( !aUseOurs || !aUseTheirs ){ rc = SQLITE_NOMEM; goto done; }
  if( !dlCoverAncestor(aAnc, nAnc, aOurs, nOurs, aUseOurs)
   || !dlCoverAncestor(aAnc, nAnc, aTheirs, nTheirs, aUseTheirs) ){
    goto done;
  }
  for(i=0; i<nTheirs; i++){
    int seen = 0;
    if( aUseTheirs[i] ) continue;
    for(j=0; j<nOurs; j++){
      if( dlChecksSame(&aTheirs[i], &aOurs[j]) ){ seen = 1; break; }
    }
    if( seen ) continue;
    for(j=0; j<nOurs; j++){
      if( aUseOurs[j] ) continue;
      if( aTheirs[i].zName && aOurs[j].zName
       && sqlite3_stricmp(aTheirs[i].zName, aOurs[j].zName)==0 ){
        goto done;
      }
      if( dlChecksOverlap(&aTheirs[i], &aOurs[j]) ) goto done;
    }
    if( pzMerged ){
      rc = DOLTLITE_GROW_ARRAY(&aAdd, &nAddAlloc, nAdd+1, 4);
      if( rc!=SQLITE_OK ) goto done;
      aAdd[nAdd++] = aTheirs[i];
    }
  }
  if( pzMerged && nAdd>0 ){
    rc = dlSpliceChecks(zOurs, aAdd, nAdd, pzMerged);
    if( rc!=SQLITE_OK ){
      if( rc!=SQLITE_NOMEM ) rc = SQLITE_OK;
      goto done;
    }
  }
  bUnion = 1;
done:
  if( rc==SQLITE_OK ) *pbUnion = bUnion;
  sqlite3_free(aAdd);
  dlChecksFree(aAnc, nAnc);
  dlChecksFree(aOurs, nOurs);
  dlChecksFree(aTheirs, nTheirs);
  sqlite3_free(aUseOurs);
  sqlite3_free(aUseTheirs);
  freeColumns(aCols, nCols);
  return rc;
}

static int dlMergeColumnChecks(
  const char *zWin, const char *zOther, ParsedColumn *aCols, int nCols,
  char **pzOut
){
  DlCheck *aChecks = 0;
  char *zCore = 0;
  sqlite3_str *pStr = 0;
  int nChecks = 0, i, rc;
  *pzOut = 0;
  rc = schemaColumnWithoutChecks(zOther, &zCore);
  if( rc==SQLITE_OK ){
    rc = dlCollectChecks(zWin, aCols, nCols, &aChecks, &nChecks);
  }
  if( rc==SQLITE_OK ){
    pStr = sqlite3_str_new(0);
    if( !pStr ) rc = SQLITE_NOMEM;
  }
  if( rc==SQLITE_OK ){
    sqlite3_str_appendall(pStr, zCore);
    for(i=0; i<nChecks; i++){
      sqlite3_str_appendf(pStr, " %s", aChecks[i].zRaw);
    }
    rc = sqlite3_str_errcode(pStr);
  }
  if( rc==SQLITE_OK ){
    *pzOut = sqlite3_str_finish(pStr);
    if( !*pzOut ) rc = SQLITE_NOMEM;
  }else{
    sqlite3_free(sqlite3_str_finish(pStr));
  }
  sqlite3_free(zCore);
  dlChecksFree(aChecks, nChecks);
  return rc;
}

static int dlComposeRetained(
  const char *zAnc, const char *zOurs, const char *zTheirs,
  int schemaChoice, const char *zTable,
  SchemaMergeAction *aAct, int nAct,
  char **pzSql, char **pzErr, int *pbConflict, int *pbHandled
);

int schemaApplyDisjointCheckUnions(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs
){
  int i, rc = SQLITE_OK;
  for(i=0; i<nOurs && rc==SQLITE_OK; i++){
    SchemaEntry *pOurs = &aOurs[i];
    SchemaEntry *pAnc, *pTheirs;
    char *zNew = 0;
    int bUnion = 0;
    if( !pOurs->zName || !pOurs->zSql || !pOurs->zType ) continue;
    if( strcmp(pOurs->zType, "table")!=0 ) continue;
    pAnc = findSchemaEntry(aAnc, nAnc, pOurs->zName);
    pTheirs = findSchemaEntry(aTheirs, nTheirs, pOurs->zName);
    if( !pAnc || !pAnc->zSql || !pTheirs || !pTheirs->zSql ) continue;
    if( !schemaEntryChangedByName(aAnc, nAnc, aOurs, nOurs, pOurs->zName)
     || !schemaEntryChangedByName(aAnc, nAnc, aTheirs, nTheirs, pOurs->zName) ){
      continue;
    }
    rc = dlComposeDisjointChecks(pAnc->zSql, pOurs->zSql, pTheirs->zSql,
                                 &zNew, &bUnion);
    if( rc==SQLITE_OK && bUnion && zNew ){
      sqlite3_free(pOurs->zSql);
      pOurs->zSql = zNew;
      zNew = 0;
    }
    sqlite3_free(zNew);
    if( rc!=SQLITE_OK ) break;
    {
      char *zKept = 0, *zErr = 0;
      int bConflict = 0, bHandled = 0;
      rc = dlComposeRetained(pAnc->zSql, pOurs->zSql, pTheirs->zSql,
                             SCHEMA_MERGE_DEFAULT, pOurs->zName,
                             0, 0, &zKept, &zErr, &bConflict, &bHandled);
      sqlite3_free(zErr);
      if( rc==SQLITE_OK && bHandled && !bConflict && zKept ){
        sqlite3_free(pOurs->zSql);
        pOurs->zSql = zKept;
      }else{
        sqlite3_free(zKept);
      }
    }
  }
  return rc;
}

/* ---- retained CHECK / FOREIGN KEY / DEFAULT composition ---- */


static DlCheck *dlCheckByName(DlCheck *a, int n, const char *zName){
  int i;
  for(i=0; i<n; i++){
    DlCheck *p = &a[i];
    if( zName && p->zName && sqlite3_stricmp(p->zName, zName)==0 ) return p;
  }
  return 0;
}

static int dlCheckIsNew(const DlCheck *p, DlCheck *aAnc, int nAnc){
  int i;
  for(i=0; i<nAnc; i++){
    if( dlChecksSame(p, &aAnc[i]) ) return 0;
    if( p->zName && aAnc[i].zName
     && sqlite3_stricmp(p->zName, aAnc[i].zName)==0 ) return 0;
  }
  return 1;
}

static int dlCutCheck(char **pzSql, const DlCheck *p){
  char *zSql = *pzSql;
  char *start = zSql + p->iOffset;
  char *end = start + strlen(p->zRaw);
  char *zNew;
  while( start>zSql && sqlite3Isspace(start[-1]) ) start--;
  if( start>zSql && start[-1]==',' ){
    start--;
  }else{
    start = zSql + p->iOffset;
  }
  zNew = sqlite3_mprintf("%.*s%s", (int)(start-zSql), zSql, end);
  if( !zNew ) return SQLITE_NOMEM;
  sqlite3_free(zSql);
  *pzSql = zNew;
  return SQLITE_OK;
}

static int dlHalfGone(DlCheck *aAnc, int nAnc, DlCheck *aA, int nA,
                      DlCheck *aB, int nB){
  int i, j;
  for(i=0; i<nAnc; i++){
    int inA = 0, inB = 0;
    if( !aAnc[i].zName ) continue;
    for(j=0; j<nA; j++){
      if( dlChecksSame(&aAnc[i], &aA[j])
       || (aAnc[i].zName && aA[j].zName
           && sqlite3_stricmp(aAnc[i].zName, aA[j].zName)==0) ) inA = 1;
    }
    for(j=0; j<nB; j++){
      if( dlChecksSame(&aAnc[i], &aB[j])
       || (aAnc[i].zName && aB[j].zName
           && sqlite3_stricmp(aAnc[i].zName, aB[j].zName)==0) ) inB = 1;
    }
    if( inA!=inB ) return 1;
  }
  return 0;
}

#define DL_SKIP 0
#define DL_KEEP 1
#define DL_REPLACE 2
#define DL_CONFLICT 3

static int dlOtherFate(const DlCheck *p, DlCheck *aWin, int nWin,
                       DlCheck *aAnc, int nAnc){
  DlCheck *pWin, *pAnc;
  int i, j;
  for(i=0; i<nWin; i++){
    if( dlChecksSame(p, &aWin[i]) ) return DL_SKIP;
  }
  for(i=0; i<nAnc; i++){
    if( !dlChecksSame(p, &aAnc[i]) ) continue;
    for(j=0; j<nWin; j++){
      if( dlChecksSame(&aAnc[i], &aWin[j]) ) break;
    }
    if( j==nWin ) return DL_SKIP;
  }
  pWin = dlCheckByName(aWin, nWin, p->zName);
  pAnc = dlCheckByName(aAnc, nAnc, p->zName);
  if( pWin && pAnc && dlChecksSame(pAnc, pWin) && !dlChecksSame(pAnc, p) ){
    return DL_REPLACE;
  }
  if( pWin && pAnc && !dlChecksSame(pAnc, pWin) && !dlChecksSame(pAnc, p)
   && !dlChecksSame(pWin, p) ){
    return DL_CONFLICT;
  }
  if( pWin && !dlChecksSame(p, pWin) ) return DL_CONFLICT;
  if( dlCheckIsNew(p, aAnc, nAnc) ){
    for(i=0; i<nWin; i++){
      if( !dlCheckIsNew(&aWin[i], aAnc, nAnc) ) continue;
      if( p->zName && aWin[i].zName
       && sqlite3_stricmp(p->zName, aWin[i].zName)==0
       && !dlChecksSame(p, &aWin[i]) ) return DL_CONFLICT;
      if( dlChecksOverlap(p, &aWin[i]) ) return DL_CONFLICT;
    }
  }
  return DL_KEEP;
}


/* Compose one-sided defaults and the checks/FKs Dolt keeps.
** *pzSql is the winning CREATE, or NULL when nothing changes.
** A kept check that names a column the merge drops sets *pbConflict
** and *pzErr. An FK in that spot is omitted. *pbHandled is 1 when the
** only differences are clauses this function can apply, so a master-row
** conflict over them is not a schema conflict. */
static int dlComposeRetained(
  const char *zAnc, const char *zOurs, const char *zTheirs,
  int schemaChoice, const char *zTable,
  SchemaMergeAction *aAct, int nAct,
  char **pzSql, char **pzErr, int *pbConflict, int *pbHandled
){
  const char *zWin, *zOth;
  ParsedColumn *aAnc = 0, *aOurs = 0, *aTheirs = 0, *aUnion = 0;
  ParsedColumn *aWin, *aOth;
  DlCheck *aCkAnc = 0, *aCkWin = 0, *aCkOth = 0;
  DlFk *aFkAnc = 0, *aFkWin = 0, *aFkOth = 0;
  char **azMerged = 0, **azRepName = 0, **azRepDef = 0;
  char **azCut = 0, **azSplice = 0, **azDefer = 0;
  char *zWork = 0;
  u8 *aReplaced = 0;
  int nAnc = 0, nOurs = 0, nTheirs = 0, nUnion = 0;
  int nWin, nOth, nCkAnc = 0, nCkWin = 0, nCkOth = 0;
  int nFkAnc = 0, nFkWin = 0, nFkOth = 0, nMerged = 0;
  int nRep = 0, nNameAlloc = 0, nDefAlloc = 0, nCut = 0, nCutAlloc = 0;
  int nSplice = 0, nSpliceAlloc = 0, nDefer = 0, nDeferAlloc = 0;
  int i, rc, bConflict = 0, bHandled = 0, changed = 0;
  int bNeutral = 0, bCoreDiff = 0, bHalf = 0;
  int uniqueChoice = SCHEMA_MERGE_DEFAULT;

  if( pzSql ) *pzSql = 0;
  if( pbConflict ) *pbConflict = 0;
  if( pbHandled ) *pbHandled = 0;
  if( !zAnc || !zOurs || !zTheirs ) return SQLITE_OK;
  zWin = schemaChoice==SCHEMA_MERGE_THEIRS ? zTheirs : zOurs;
  zOth = schemaChoice==SCHEMA_MERGE_THEIRS ? zOurs : zTheirs;

  rc = parseColumns(zAnc, &aAnc, &nAnc);
  if( rc==SQLITE_OK ) rc = parseColumns(zOurs, &aOurs, &nOurs);
  if( rc==SQLITE_OK ) rc = parseColumns(zTheirs, &aTheirs, &nTheirs);
  if( rc!=SQLITE_OK ){
    if( rc!=SQLITE_NOMEM ) rc = SQLITE_OK;
    goto done;
  }
  aWin = schemaChoice==SCHEMA_MERGE_THEIRS ? aTheirs : aOurs;
  aOth = schemaChoice==SCHEMA_MERGE_THEIRS ? aOurs : aTheirs;
  nWin = schemaChoice==SCHEMA_MERGE_THEIRS ? nTheirs : nOurs;
  nOth = schemaChoice==SCHEMA_MERGE_THEIRS ? nOurs : nTheirs;

  rc = dlNeutralSame(zAnc, zOurs, &bNeutral);
  if( rc==SQLITE_OK && bNeutral ) rc = dlNeutralSame(zAnc, zTheirs, &bNeutral);
  if( rc!=SQLITE_OK ){
    if( rc!=SQLITE_NOMEM ) rc = SQLITE_OK;
    goto done;
  }

  if( schemaChoice!=SCHEMA_MERGE_DEFAULT ){
    rc = schemaUniqueSideChoice(zAnc, zOurs, zTheirs, &uniqueChoice);
    if( rc!=SQLITE_OK ) goto done;
  }

  for(i=0; i<nWin; i++){
    int j = parsedColumnIndexByName(aOth, nOth, aWin[i].zName);
    int k, cores, same;
    if( j<0 ) continue;
    if( schemaDefinitionsEquivalent(aWin[i].zDef, aOth[j].zDef) ) continue;
    cores = dlCoresMatch(aWin[i].zDef, aOth[j].zDef,
        uniqueChoice!=SCHEMA_MERGE_DEFAULT && uniqueChoice==schemaChoice);
    if( cores<0 ){
      rc = -cores==SQLITE_NOMEM ? SQLITE_NOMEM : SQLITE_OK;
      goto done;
    }
    k = parsedColumnIndexByName(aAnc, nAnc, aWin[i].zName);
    if( !cores ){
      bCoreDiff = 1;
      if( k>=0 ){
        bConflict = 1;
        if( pzErr && !*pzErr ){
          *pzErr = sqlite3_mprintf("incompatible definitions for column '%s'",
                                   aWin[i].zName);
          if( !*pzErr ){ rc = SQLITE_NOMEM; goto done; }
        }
      }
      continue;
    }
    if( k<0 ) continue;
    rc = schemaColumnNonCheckTextMatches(aAnc[k].zDef, aWin[i].zDef, &same);
    if( rc!=SQLITE_OK ) goto done;
    if( !same ) continue;
    rc = schemaColumnNonCheckTextMatches(aWin[i].zDef, aOth[j].zDef, &same);
    if( rc!=SQLITE_OK ) goto done;
    if( same ) continue;
    rc = DOLTLITE_GROW_ARRAY(&azRepName, &nNameAlloc, nRep+1, 4);
    if( rc==SQLITE_OK ){
      rc = DOLTLITE_GROW_ARRAY(&azRepDef, &nDefAlloc, nRep+1, 4);
    }
    if( rc!=SQLITE_OK ) goto done;
    azRepName[nRep] = aWin[i].zName;
    rc = dlMergeColumnChecks(aWin[i].zDef, aOth[j].zDef,
                             aWin, nWin, &azRepDef[nRep]);
    if( rc!=SQLITE_OK ) goto done;
    nRep++;
  }

  rc = dlRewriteColumns(zWin, azRepName, azRepDef, nRep, &zWork, &changed);
  if( rc!=SQLITE_OK ){
    if( rc!=SQLITE_NOMEM ) rc = SQLITE_OK;
    goto done;
  }
  rc = dlMergedNames(aAnc, nAnc, aWin, nWin, aOth, nOth, &azMerged, &nMerged);
  if( rc!=SQLITE_OK ) goto done;
  rc = dlUnionCols(aAnc, nAnc, aOurs, nOurs, aTheirs, nTheirs, &aUnion, &nUnion);
  if( rc!=SQLITE_OK ) goto done;
  rc = dlCollectChecks(zAnc, aUnion, nUnion, &aCkAnc, &nCkAnc);
  if( rc==SQLITE_OK ){
    rc = dlCollectChecks(zWork, aUnion, nUnion, &aCkWin, &nCkWin);
  }
  if( rc==SQLITE_OK ){
    rc = dlCollectChecks(zOth, aUnion, nUnion, &aCkOth, &nCkOth);
  }
  if( rc!=SQLITE_OK ){
    if( rc!=SQLITE_NOMEM ) rc = SQLITE_OK;
    goto done;
  }
  if( dlHalfGone(aCkAnc, nCkAnc, aCkWin, nCkWin, aCkOth, nCkOth) ) bHalf = 1;
  if( nCkWin ){
    aReplaced = sqlite3_malloc(nCkWin);
    if( !aReplaced ){ rc = SQLITE_NOMEM; goto done; }
    memset(aReplaced, 0, (size_t)nCkWin);
  }

  for(i=0; i<nCkOth; i++){
    int fate = dlOtherFate(&aCkOth[i], aCkWin, nCkWin, aCkAnc, nCkAnc);
    int cls;
    DlCheck *pOld;
    if( fate==DL_SKIP ) continue;
    if( fate==DL_CONFLICT ){
      bConflict = 1;
      if( pzErr && !*pzErr ){
        *pzErr = sqlite3_mprintf("incompatible check constraints");
        if( !*pzErr ){ rc = SQLITE_NOMEM; goto done; }
      }
      continue;
    }
    cls = dlRefClass(aCkOth[i].zCols, azMerged, nMerged, aWin, nWin);
    if( cls<0 ){ rc = SQLITE_NOMEM; goto done; }
    if( cls==2 ){
      bConflict = 1;
      if( pzErr && !*pzErr ){
        *pzErr = sqlite3_mprintf(
            "check '%s' references a column that will be deleted after merge",
            aCkOth[i].zName ? aCkOth[i].zName : "");
        if( !*pzErr ){ rc = SQLITE_NOMEM; goto done; }
      }
      continue;
    }
    if( fate==DL_REPLACE ){
      pOld = dlCheckByName(aCkWin, nCkWin, aCkOth[i].zName);
      if( pOld && pOld->zRaw ){
        int idx = (int)(pOld - aCkWin);
        if( idx>=0 && idx<nCkWin ) aReplaced[idx] = 1;
        rc = dlPushRaw(&azCut, &nCut, &nCutAlloc, pOld->zRaw);
        if( rc!=SQLITE_OK ) goto done;
      }
    }
    if( cls==1 ){
      rc = dlPushRaw(&azDefer, &nDefer, &nDeferAlloc, aCkOth[i].zRaw);
    }else if( !aCkOth[i].zRaw || !dlFindClause(zWork, aCkOth[i].zRaw) ){
      rc = dlPushRaw(&azSplice, &nSplice, &nSpliceAlloc, aCkOth[i].zRaw);
    }
    if( rc!=SQLITE_OK ) goto done;
  }

  for(i=0; i<nCkWin && !bConflict; i++){
    int cls, j;
    if( aReplaced && aReplaced[i] ) continue;
    if( !aCkWin[i].zName ){
      for(j=0; j<nCkAnc; j++){
        if( dlChecksSame(&aCkWin[i], &aCkAnc[j]) ) break;
      }
      if( j<nCkAnc ){
        for(j=0; j<nCkOth; j++){
          if( dlChecksSame(&aCkWin[i], &aCkOth[j]) ) break;
        }
        if( j==nCkOth ){
          aReplaced[i] = 2;
          continue;
        }
      }
    }
    cls = dlRefClass(aCkWin[i].zCols, azMerged, nMerged, aWin, nWin);
    if( cls<0 ){ rc = SQLITE_NOMEM; goto done; }
    if( cls==2 ){
      bConflict = 1;
      if( pzErr && !*pzErr ){
        *pzErr = sqlite3_mprintf(
            "check '%s' references a column that will be deleted after merge",
            aCkWin[i].zName ? aCkWin[i].zName : "");
        if( !*pzErr ){ rc = SQLITE_NOMEM; goto done; }
      }
    }
  }

  rc = dlCollectFks(zAnc, &aFkAnc, &nFkAnc);
  if( rc==SQLITE_OK ) rc = dlCollectFks(zWork, &aFkWin, &nFkWin);
  if( rc==SQLITE_OK ) rc = dlCollectFks(zOth, &aFkOth, &nFkOth);
  if( rc!=SQLITE_OK ){
    if( rc!=SQLITE_NOMEM ) rc = SQLITE_OK;
    goto done;
  }
  for(i=0; i<nFkAnc; i++){
    int inW = 0, inO = 0, j;
    for(j=0; j<nFkWin; j++){
      if( dlFkCorresponds(&aFkAnc[i], &aFkWin[j]) ) inW = 1;
    }
    for(j=0; j<nFkOth; j++){
      if( dlFkCorresponds(&aFkAnc[i], &aFkOth[j]) ) inO = 1;
    }
    if( inW!=inO ) bHalf = 1;
  }
  for(i=0; i<nFkOth && !bConflict; i++){
    int j, cls;
    DlFk *pWin = 0, *pAnc = 0;
    for(j=0; j<nFkWin; j++){
      if( dlFkSame(&aFkOth[i], &aFkWin[j]) ) break;
    }
    if( j<nFkWin ) continue;
    for(j=0; j<nFkAnc; j++){
      if( dlFkCorresponds(&aFkAnc[j], &aFkOth[i]) ){ pAnc = &aFkAnc[j]; break; }
    }
    for(j=0; j<nFkWin; j++){
      if( dlFkCorresponds(&aFkWin[j], &aFkOth[i]) ){ pWin = &aFkWin[j]; break; }
    }
    /* The winner already dropped this constraint, including a one-sided
    ** edit of it. Leave the deletion in place. */
    if( pAnc && !pWin ) continue;
    if( pWin && !dlFkSame(pWin, &aFkOth[i]) ){
      if( !(pAnc && dlFkSame(pAnc, pWin)) ){
        bConflict = 1;
        if( pzErr && !*pzErr ){
          *pzErr = sqlite3_mprintf("incompatible foreign key constraints");
          if( !*pzErr ){ rc = SQLITE_NOMEM; goto done; }
        }
        continue;
      }
      rc = dlPushRaw(&azCut, &nCut, &nCutAlloc, pWin->zRaw);
      if( rc!=SQLITE_OK ) goto done;
    }
    cls = dlRefClass(aFkOth[i].zCols, azMerged, nMerged, aWin, nWin);
    if( cls<0 ){ rc = SQLITE_NOMEM; goto done; }
    if( cls==2 ) continue;
    if( cls==1 ){
      rc = dlPushRaw(&azDefer, &nDefer, &nDeferAlloc, aFkOth[i].zRaw);
    }else if( !aFkOth[i].zRaw || !dlFindClause(zWork, aFkOth[i].zRaw) ){
      rc = dlPushRaw(&azSplice, &nSplice, &nSpliceAlloc, aFkOth[i].zRaw);
    }
    if( rc!=SQLITE_OK ) goto done;
  }

  if( !bConflict && !bCoreDiff && !bHalf && !bNeutral ){
    const char *azSql[3] = {zAnc, zOurs, zTheirs};
    ParsedColumn *aCols[3] = {aAnc, aOurs, aTheirs};
    int anCols[3] = {nAnc, nOurs, nTheirs};
    char *azTrim[3] = {0, 0, 0};
    int j;
    for(i=0; i<3 && rc==SQLITE_OK; i++){
      char **azDrop = sqlite3_malloc64((u64)(anCols[i]+1)*sizeof(char*));
      int nDrop = 0, bChanged = 0;
      if( !azDrop ){ rc = SQLITE_NOMEM; break; }
      for(j=0; j<anCols[i]; j++){
        const char *zName = aCols[i][j].zName;
        if( parsedColumnIndexByName(aAnc,nAnc,zName)>=0
         && parsedColumnIndexByName(aOurs,nOurs,zName)>=0
         && parsedColumnIndexByName(aTheirs,nTheirs,zName)>=0 ) continue;
        azDrop[nDrop++] = aCols[i][j].zName;
      }
      rc = dlRewriteColumns(azSql[i], azDrop, 0, nDrop,
                             &azTrim[i], &bChanged);
      sqlite3_free(azDrop);
    }
    if( rc==SQLITE_OK ) rc = dlNeutralSame(azTrim[0],azTrim[1],&bNeutral);
    if( rc==SQLITE_OK && bNeutral ){
      rc = dlNeutralSame(azTrim[0],azTrim[2],&bNeutral);
    }
    for(i=0; i<3; i++) sqlite3_free(azTrim[i]);
    if( rc!=SQLITE_OK ) goto done;
  }
  if( !bConflict && bNeutral && !bCoreDiff && !bHalf ) bHandled = 1;
  if( bConflict || !pzSql ){
    changed = 0;
  }else{
    /* Remove clauses from right to left to preserve their recorded offsets. */
    for(i=nCkWin-1; i>=0 && rc==SQLITE_OK; i--){
      if( aReplaced[i]!=2 ) continue;
      rc = dlCutCheck(&zWork, &aCkWin[i]);
      changed = 1;
    }
    for(i=0; i<nCut && rc==SQLITE_OK; i++) rc = dlCutRaw(&zWork, azCut[i]);
    if( rc==SQLITE_OK && nSplice>0 ){
      DlCheck *aAdd = sqlite3_malloc(sizeof(DlCheck)*(nSplice ? nSplice : 1));
      char *zNew = 0;
      if( !aAdd ) rc = SQLITE_NOMEM;
      else{
        memset(aAdd, 0, sizeof(DlCheck)*nSplice);
        for(i=0; i<nSplice; i++) aAdd[i].zRaw = azSplice[i];
        rc = dlSpliceChecks(zWork, aAdd, nSplice, &zNew);
        sqlite3_free(aAdd);
        if( rc==SQLITE_OK && zNew ){
          sqlite3_free(zWork);
          zWork = zNew;
          changed = 1;
        }else if( rc==SQLITE_CORRUPT ){
          rc = SQLITE_OK;
        }
      }
    }
    if( rc==SQLITE_OK ){
      for(i=0; i<nDefer; i++){
        rc = dlDeferRaw(aAct, nAct, zTable, azDefer[i]);
        if( rc!=SQLITE_OK ) break;
      }
    }
    if( nCut>0 ) changed = 1;
  }

done:
  if( rc==SQLITE_OK ){
    if( pbConflict ) *pbConflict = bConflict;
    if( pbHandled ) *pbHandled = bHandled;
    if( pzSql && changed && zWork && rc==SQLITE_OK && !bConflict ){
      *pzSql = zWork;
      zWork = 0;
    }
  }
  sqlite3_free(zWork);
  dlFreeRaws(azCut, nCut);
  dlFreeRaws(azSplice, nSplice);
  dlFreeRaws(azDefer, nDefer);
  sqlite3_free(azRepName);
  for(i=0; i<nRep; i++) sqlite3_free(azRepDef[i]);
  sqlite3_free(azRepDef);
  sqlite3_free(aReplaced);
  dlFksFree(aFkAnc, nFkAnc);
  dlFksFree(aFkWin, nFkWin);
  dlFksFree(aFkOth, nFkOth);
  dlChecksFree(aCkAnc, nCkAnc);
  dlChecksFree(aCkWin, nCkWin);
  dlChecksFree(aCkOth, nCkOth);
  sqlite3_free(aUnion);
  dlFreeNames(azMerged, nMerged);
  freeColumns(aAnc, nAnc);
  freeColumns(aOurs, nOurs);
  freeColumns(aTheirs, nTheirs);
  return rc;
}

static int dlRetainedUnified(const char *zAnc, const char *zOurs,
                             const char *zTheirs, int *pbUnion){
  int bConflict = 0, bHandled = 0, rc;
  *pbUnion = 0;
  rc = dlComposeRetained(zAnc, zOurs, zTheirs, SCHEMA_MERGE_DEFAULT,
                         0, 0, 0, 0, 0, &bConflict, &bHandled);
  if( rc==SQLITE_OK && bHandled && !bConflict ) *pbUnion = 1;
  return rc;
}

int schemaRetainedClauseConflict(
  const char *zAnc, const char *zOurs, const char *zTheirs,
  int schemaChoice, char **pzErr
){
  int bConflict = 0, bHandled = 0, rc;
  rc = dlComposeRetained(zAnc, zOurs, zTheirs, schemaChoice,
                         0, 0, 0, 0, pzErr, &bConflict, &bHandled);
  if( rc!=SQLITE_OK ) return rc;
  return bConflict ? SQLITE_ERROR : SQLITE_OK;
}

int schemaAdoptMergedTableSql(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs,
  const char *zName, const char *zFallback, int iTable,
  int schemaChoice, char **pzOursPrev,
  SchemaMergeAction *aAct, int nAct
){
  SchemaEntry *pOurs, *pTheirs, *pAnc;
  const char *zAncSql, *zOursSql, *zTheirsSql;
  char *zNew = 0, *zErr = 0;
  int bConflict = 0, bHandled = 0, rc = SQLITE_OK;

  pOurs = zName ? findSchemaEntry(aOurs, nOurs, zName) : 0;
  if( !pOurs && zFallback ) pOurs = findSchemaEntry(aOurs, nOurs, zFallback);
  pTheirs = zName ? findSchemaEntry(aTheirs, nTheirs, zName) : 0;
  if( !pTheirs && zFallback ){
    pTheirs = findSchemaEntry(aTheirs, nTheirs, zFallback);
  }
  if( !pTheirs ) pTheirs = findSchemaEntryByRootpage(aTheirs, nTheirs, iTable);
  pAnc = zName ? findSchemaEntry(aAnc, nAnc, zName) : 0;
  if( !pAnc && zFallback ) pAnc = findSchemaEntry(aAnc, nAnc, zFallback);

  zAncSql = pAnc && pAnc->zSql ? pAnc->zSql : 0;
  zOursSql = pOurs && pOurs->zSql ? pOurs->zSql : 0;
  zTheirsSql = pTheirs && pTheirs->zSql ? pTheirs->zSql : 0;

  if( schemaChoice==SCHEMA_MERGE_THEIRS ){
    char *zCopy = zTheirsSql ? sqlite3_mprintf("%s", zTheirsSql) : 0;
    if( !pOurs || !zCopy ){
      sqlite3_free(zCopy);
      return pOurs ? SQLITE_NOMEM : SQLITE_CORRUPT;
    }
    if( pzOursPrev ) *pzOursPrev = pOurs->zSql;
    pOurs->zSql = zCopy;
  }

  if( !pOurs || !pOurs->zType || strcmp(pOurs->zType, "table")!=0 ){
    return SQLITE_OK;
  }
  if( !zAncSql || !zOursSql || !zTheirsSql ) return SQLITE_OK;
  rc = dlComposeRetained(zAncSql, zOursSql, zTheirsSql, schemaChoice,
                         zName ? zName : zFallback, aAct, nAct,
                         &zNew, &zErr, &bConflict, &bHandled);
  sqlite3_free(zErr);
  if( rc!=SQLITE_OK || bConflict || !zNew ){
    sqlite3_free(zNew);
    return rc;
  }
  sqlite3_free(pOurs->zSql);
  pOurs->zSql = zNew;
  return SQLITE_OK;
}

int schemaInstallDeferredClauses(
  sqlite3 *db, const char *zTable, char **azClauses, int nClauses
){
  sqlite3_stmt *pSel = 0, *pUp = 0;
  DlCheck *aAdd = 0;
  const char *zSql;
  char *zNew = 0;
  u64 savedFlags;
  int i, nAdd = 0, rc;
  if( nClauses<=0 || !zTable ) return SQLITE_OK;
  rc = sqlite3_prepare_v2(db,
      "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
      -1, &pSel, 0);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_bind_text(pSel, 1, zTable, -1, SQLITE_STATIC);
  if( sqlite3_step(pSel)!=SQLITE_ROW ){
    sqlite3_finalize(pSel);
    return SQLITE_OK;
  }
  zSql = (const char*)sqlite3_column_text(pSel, 0);
  if( !zSql ){
    sqlite3_finalize(pSel);
    return SQLITE_OK;
  }
  aAdd = sqlite3_malloc(sizeof(DlCheck) * nClauses);
  if( !aAdd ){
    sqlite3_finalize(pSel);
    return SQLITE_NOMEM;
  }
  memset(aAdd, 0, sizeof(DlCheck) * nClauses);
  for(i=0; i<nClauses; i++){
    if( !azClauses[i] || !azClauses[i][0] ) continue;
    if( dlFindClause(zSql, azClauses[i]) ) continue;
    aAdd[nAdd].zRaw = azClauses[i];
    nAdd++;
  }
  if( nAdd==0 ){
    sqlite3_free(aAdd);
    sqlite3_finalize(pSel);
    return SQLITE_OK;
  }
  rc = dlSpliceChecks(zSql, aAdd, nAdd, &zNew);
  sqlite3_free(aAdd);
  sqlite3_finalize(pSel);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zNew);
    return rc;
  }
  /* Defensive mode ignores PRAGMA writable_schema=ON. The flag itself
  ** is what lets this one schema-text update through. */
  savedFlags = db->flags;
  db->flags = (savedFlags & ~(u64)SQLITE_Defensive) | SQLITE_WriteSchema;
  rc = sqlite3_prepare_v2(db,
      "UPDATE sqlite_master SET sql=? WHERE type='table' AND name=?",
      -1, &pUp, 0);
  if( rc==SQLITE_OK ){
    char *zKeep = 0;
    sqlite3_bind_text(pUp, 1, zNew, -1, SQLITE_STATIC);
    sqlite3_bind_text(pUp, 2, zTable, -1, SQLITE_STATIC);
    rc = sqlite3_step(pUp);
    if( rc==SQLITE_DONE ) rc = SQLITE_OK;
    if( rc!=SQLITE_OK ) zKeep = sqlite3_mprintf("%s", sqlite3_errmsg(db));
    sqlite3_finalize(pUp);
    if( zKeep ){
      sqlite3ErrorWithMsg(db, rc, "%s", zKeep);
      sqlite3_free(zKeep);
    }
  }
  db->flags = savedFlags;
  sqlite3_free(zNew);
  return rc;
}

int schemaTableChecksUnified(
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs,
  const char *zName,
  int *pbUnion
){
  SchemaEntry *pAnc, *pOurs, *pTheirs;
  int rc;
  *pbUnion = 0;
  if( !zName ) return SQLITE_OK;
  pAnc = findSchemaEntry(aAnc, nAnc, zName);
  pOurs = findSchemaEntry(aOurs, nOurs, zName);
  pTheirs = findSchemaEntry(aTheirs, nTheirs, zName);
  if( !pAnc || !pAnc->zSql || !pOurs || !pOurs->zSql
   || !pTheirs || !pTheirs->zSql ){
    return SQLITE_OK;
  }
  rc = dlComposeDisjointChecks(pAnc->zSql, pOurs->zSql, pTheirs->zSql,
                               0, pbUnion);
  if( rc!=SQLITE_OK || *pbUnion ) return rc;
  return dlRetainedUnified(pAnc->zSql, pOurs->zSql, pTheirs->zSql, pbUnion);
}

#endif
