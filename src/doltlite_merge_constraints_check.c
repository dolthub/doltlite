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

/* A CHECK clause. zCols is newline-separated referenced column names. */
typedef struct DlCheck DlCheck;
struct DlCheck {
  char *zName;
  char *zExpr;
  char *zRaw;
  char *zCols;
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

static int dlColsContain(const char *zCols, const char *zName){
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
  const char *z,
  int n
){
  char *zName;
  char *zNew;
  int i;
  zName = sqlite3_mprintf("%.*s", n, z);
  if( !zName ) return SQLITE_NOMEM;
  sqlite3Dequote(zName);
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

static int dlCheckNoteCols(DlCheck *p, ParsedColumn *aCols, int nCols){
  const char *z = p->zExpr ? p->zExpr : "";
  const char *zEnd = z + strlen(z);
  while( z<zEnd ){
    int type = 0, n, qt = 0, qn;
    const char *q;
    n = sqlite3GetToken((const u8*)z, &type);
    if( n<=0 || type==TK_ILLEGAL ) return SQLITE_CORRUPT;
    q = z + n;
    if( type!=TK_SPACE && type!=TK_COMMENT && type!=TK_STRING
     && type!=TK_BLOB && type!=TK_INTEGER && type!=TK_FLOAT ){
      while( q<zEnd ){
        qn = sqlite3GetToken((const u8*)q, &qt);
        if( qn<=0 || (qt!=TK_SPACE && qt!=TK_COMMENT) ) break;
        q += qn;
      }
      if( !(q<zEnd && qt==TK_LP) ){
        int rc = dlCheckAddCol(p, aCols, nCols, z, n);
        if( rc!=SQLITE_OK ) return rc;
      }
    }
    z += n;
  }
  return SQLITE_OK;
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
    }else{
      sqlite3_free(zNew);
    }
  }
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
  *pbUnion = 0;
  if( !zName ) return SQLITE_OK;
  pAnc = findSchemaEntry(aAnc, nAnc, zName);
  pOurs = findSchemaEntry(aOurs, nOurs, zName);
  pTheirs = findSchemaEntry(aTheirs, nTheirs, zName);
  if( !pAnc || !pAnc->zSql || !pOurs || !pOurs->zSql
   || !pTheirs || !pTheirs->zSql ){
    return SQLITE_OK;
  }
  return dlComposeDisjointChecks(pAnc->zSql, pOurs->zSql, pTheirs->zSql,
                                 0, pbUnion);
}

#endif
